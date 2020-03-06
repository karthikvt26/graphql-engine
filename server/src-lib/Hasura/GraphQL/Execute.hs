module Hasura.GraphQL.Execute
  ( GQExecPlan(..)

  , ExecPlanPartial
  , getExecPlanPartial

  , ExecOp(..)
  , ExecPlanResolved
  , getResolvedExecPlan
  , execRemoteGQ
  , getSubsOp

  , EP.PlanCache
  , EP.mkPlanCacheOptions
  , EP.PlanCacheOptions
  , EP.initPlanCache
  , EP.clearPlanCache
  , EP.dumpPlanCache

  , ExecutionCtx(..)
  , GQLApiAuthorization(..)
  ) where

import           Control.Exception                      (try)
import           Control.Lens
import           Data.Has

import qualified Data.Aeson                             as J
import qualified Data.CaseInsensitive                   as CI
import qualified Data.HashMap.Strict                    as Map
import qualified Data.HashSet                           as Set
import qualified Data.String.Conversions                as CS
import qualified Data.Text                              as T
import qualified Database.PG.Query                      as Q
import qualified Language.GraphQL.Draft.Syntax          as G
import qualified Network.HTTP.Client                    as HTTP
import qualified Network.HTTP.Types                     as HTTP
import qualified Network.Wreq                           as Wreq

import           Hasura.EncJSON
import           Hasura.GraphQL.Context
import           Hasura.GraphQL.Logging
import           Hasura.GraphQL.Resolve.Context
import           Hasura.GraphQL.Schema
import           Hasura.GraphQL.Transport.HTTP.Protocol
import           Hasura.GraphQL.Validate.Types
import           Hasura.HTTP
import           Hasura.Prelude
import           Hasura.RQL.DDL.Headers
import           Hasura.RQL.Types
import           Hasura.Server.Context
import           Hasura.Server.Utils                    (IpAddress, RequestId, filterRequestHeaders)
import           Hasura.Server.Version                  (HasVersion)

import qualified Hasura.GraphQL.Execute.LiveQuery       as EL
import qualified Hasura.GraphQL.Execute.Plan            as EP
import qualified Hasura.GraphQL.Execute.Query           as EQ
import qualified Hasura.GraphQL.Resolve                 as GR
import qualified Hasura.GraphQL.Validate                as VQ
import qualified Hasura.GraphQL.Validate.Types          as VT
import qualified Hasura.Logging                         as L
import qualified Hasura.Server.Telemetry.Counters       as Telem

-- The current execution plan of a graphql operation, it is
-- currently, either local pg execution or a remote execution
--
-- The 'a' is parameterised so this AST can represent
-- intermediate passes
data GQExecPlan a
  = GExPHasura !a
  | GExPRemote !RemoteSchemaInfo !G.TypedOperationDefinition
  deriving (Functor, Foldable, Traversable)

-- | Execution context
data ExecutionCtx
  = ExecutionCtx
  { _ecxLogger          :: !(L.Logger L.Hasura)
  , _ecxSqlGenCtx       :: !SQLGenCtx
  , _ecxPgExecCtx       :: !IsPGExecCtx
  , _ecxPlanCache       :: !EP.PlanCache
  , _ecxSchemaCache     :: !SchemaCache
  , _ecxSchemaCacheVer  :: !SchemaCacheVer
  , _ecxHttpManager     :: !HTTP.Manager
  , _ecxEnableAllowList :: !Bool
  }

-- | Typeclass representing rules/authorization to be enforced on the GraphQL API (over both HTTP & Websockets)
-- | This is separate from the permissions system. Permissions apply on GraphQL related objects, but
-- this is applicable on other general things
class Monad m => GQLApiAuthorization m where
  authorizeGQLApi
    :: UserInfo
    -> ([HTTP.Header], IpAddress)
    -- ^ request headers and IP address
    -> GQLReqUnparsed
    -- ^ the unparsed GraphQL query string (and related values)
    -> m (Either QErr GQLReqParsed)
    -- ^ after enforcing authorization, it should return the parsed GraphQL query

-- Enforces the current limitation
assertSameLocationNodes
  :: (MonadError QErr m) => [VT.TypeLoc] -> m VT.TypeLoc
assertSameLocationNodes typeLocs =
  case Set.toList (Set.fromList typeLocs) of
    -- this shouldn't happen
    []    -> return VT.TLHasuraType
    [loc] -> return loc
    _     -> throw400 NotSupported msg
  where
    msg = "cannot mix top level fields from two different graphql servers"

-- TODO: we should fix this function asap
-- as this will fail when there is a fragment at the top level
getTopLevelNodes :: G.TypedOperationDefinition -> [G.Name]
getTopLevelNodes opDef =
  mapMaybe f $ G._todSelectionSet opDef
  where
    f = \case
      G.SelectionField fld        -> Just $ G._fName fld
      G.SelectionFragmentSpread _ -> Nothing
      G.SelectionInlineFragment _ -> Nothing

gatherTypeLocs :: GCtx -> [G.Name] -> [VT.TypeLoc]
gatherTypeLocs gCtx nodes =
  catMaybes $ flip map nodes $ \node ->
    VT._fiLoc <$> Map.lookup node schemaNodes
  where
    schemaNodes =
      let qr = VT._otiFields $ _gQueryRoot gCtx
          mr = VT._otiFields <$> _gMutRoot gCtx
      in maybe qr (Map.union qr) mr

-- This is for when the graphql query is validated
type ExecPlanPartial = GQExecPlan (GCtx, VQ.RootSelSet, Q.TxAccess)

getExecPlanPartial
  :: (MonadReusability m, MonadError QErr m)
  => UserInfo
  -> SchemaCache
  -> Bool
  -> GQLReqParsed
  -> m ExecPlanPartial
getExecPlanPartial userInfo sc enableAL req = do

  -- check if query is in allowlist
  when enableAL checkQueryInAllowlist

  gCtx <- flip runCacheRT sc $ getGCtx role gCtxRoleMap
  queryParts <- flip runReaderT gCtx $ VQ.getQueryParts req

  let opDef = VQ.qpOpDef queryParts
      topLevelNodes = getTopLevelNodes opDef
      -- gather TypeLoc of topLevelNodes
      typeLocs = gatherTypeLocs gCtx topLevelNodes

  -- see if they are all the same
  typeLoc <- assertSameLocationNodes typeLocs

  case typeLoc of
    VT.TLHasuraType -> do
      rootSelSet <- runReaderT (VQ.validateGQ queryParts) gCtx
      txAccess <- getTxAccess rootSelSet
      return $ GExPHasura (gCtx, rootSelSet, txAccess)
    VT.TLRemoteType _ rsi ->
      return $ GExPRemote rsi opDef
  where
    role = userRole userInfo
    gCtxRoleMap = scGCtxMap sc
    getTxAccess rootSelSet = case rootSelSet of
      VQ.RQuery{}        -> getGQLQueryTxAccess rootSelSet
      VQ.RMutation{}     -> return Q.ReadWrite
      VQ.RSubscription{} -> return Q.ReadOnly

    -- TODO: This function should ideally read a configuration
    -- in the schema cache to get the transaction access to be used to
    -- for executing the query. For now, we default to Q.readOnly
    getGQLQueryTxAccess _ = return Q.ReadOnly
    checkQueryInAllowlist =
      -- only for non-admin roles
      when (role /= adminRole) $ do
        let notInAllowlist =
              not $ VQ.isQueryInAllowlist (_grQuery req) (scAllowlist sc)
        when notInAllowlist $ modifyQErr modErr $ throwVE "query is not allowed"

    modErr e =
      let msg = "query is not in any of the allowlists"
      in e{qeInternal = Just $ J.object [ "message" J..= J.String msg]}


-- An execution operation, in case of
-- queries and mutations it is just a transaction
-- to be executed
data ExecOp
  = ExOpQuery !LazyRespTx !(Maybe EQ.GeneratedSqlMap)
  | ExOpMutation !LazyRespTx
  | ExOpSubs !EL.LiveQueryPlan

-- The graphql query is resolved into an execution operation
type ExecPlanResolved
  = GQExecPlan (ExecOp, Q.TxAccess)

getResolvedExecPlan
  :: (MonadError QErr m, MonadIO m)
  => IsPGExecCtx
  -> EP.PlanCache
  -> UserInfo
  -> SQLGenCtx
  -> Bool
  -> SchemaCache
  -> SchemaCacheVer
  -> (GQLReqUnparsed, GQLReqParsed)
  -> m (Telem.CacheHit, ExecPlanResolved)
getResolvedExecPlan isPgCtx planCache userInfo sqlGenCtx
  enableAL sc scVer (reqUnparsed, reqParsed) = do

  planM <- liftIO $ EP.getPlan scVer (userRole userInfo)
           opNameM queryStr planCache
  let usrVars = userVars userInfo
  case planM of
    -- plans are only for queries and subscriptions
    Just plan -> (Telem.Hit,) . GExPHasura <$> case plan of
      EP.RPQuery queryPlan txAccess -> do
        (tx, genSql) <- EQ.queryOpFromPlan usrVars queryVars queryPlan
        return (ExOpQuery tx $ Just genSql, txAccess)
      EP.RPSubs subsPlan txAccess ->
        (, txAccess) . ExOpSubs <$> EL.reuseLiveQueryPlan isPgCtx usrVars queryVars subsPlan
    Nothing -> (Telem.Miss,) <$> noExistingPlan
  where
    GQLReq opNameM queryStr queryVars = reqUnparsed
    addPlanToCache plan =
      liftIO $ EP.addPlan scVer (userRole userInfo)
      opNameM queryStr plan planCache
    noExistingPlan = do
      (partialExecPlan, queryReusability) <- runReusabilityT $
        getExecPlanPartial userInfo sc enableAL reqParsed
      forM partialExecPlan $ \(gCtx, rootSelSet, txAccess) ->
        case rootSelSet of
          VQ.RMutation selSet ->
            (, Q.ReadWrite) . ExOpMutation <$> getMutOp gCtx sqlGenCtx userInfo selSet
          VQ.RQuery selSet -> do
            (queryTx, plan, genSql) <- getQueryOp gCtx sqlGenCtx userInfo queryReusability selSet
            traverse_ (addPlanToCache . flip EP.RPQuery txAccess) plan
            return (ExOpQuery queryTx $ Just genSql, txAccess)
          VQ.RSubscription fld -> do
            (lqOp, plan) <- getSubsOp isPgCtx gCtx sqlGenCtx userInfo queryReusability fld
            traverse_ (addPlanToCache . flip EP.RPSubs txAccess) plan
            return $ (ExOpSubs lqOp, txAccess)

-- Monad for resolving a hasura query/mutation
type E m =
  ReaderT ( UserInfo
          , QueryCtxMap
          , MutationCtxMap
          , TypeMap
          , FieldMap
          , OrdByCtx
          , InsCtxMap
          , SQLGenCtx
          ) (ExceptT QErr m)

runE
  :: (MonadError QErr m)
  => GCtx
  -> SQLGenCtx
  -> UserInfo
  -> E m a
  -> m a
runE ctx sqlGenCtx userInfo action = do
  res <- runExceptT $ runReaderT action
    (userInfo, queryCtxMap, mutationCtxMap, typeMap, fldMap, ordByCtx, insCtxMap, sqlGenCtx)
  either throwError return res
  where
    queryCtxMap = _gQueryCtxMap ctx
    mutationCtxMap = _gMutationCtxMap ctx
    typeMap = _gTypes ctx
    fldMap = _gFields ctx
    ordByCtx = _gOrdByCtx ctx
    insCtxMap = _gInsCtxMap ctx

getQueryOp
  :: (MonadError QErr m)
  => GCtx
  -> SQLGenCtx
  -> UserInfo
  -> QueryReusability
  -> VQ.SelSet
  -> m (LazyRespTx, Maybe EQ.ReusableQueryPlan, EQ.GeneratedSqlMap)
getQueryOp gCtx sqlGenCtx userInfo queryReusability fields =
  runE gCtx sqlGenCtx userInfo $ EQ.convertQuerySelSet queryReusability fields

mutationRootName :: Text
mutationRootName = "mutation_root"

resolveMutSelSet
  :: ( MonadError QErr m
     , MonadReader r m
     , Has UserInfo r
     , Has MutationCtxMap r
     , Has FieldMap r
     , Has OrdByCtx r
     , Has SQLGenCtx r
     , Has InsCtxMap r
     )
  => VQ.SelSet
  -> m LazyRespTx
resolveMutSelSet fields = do
  aliasedTxs <- forM (toList fields) $ \fld -> do
    fldRespTx <- case VQ._fName fld of
      "__typename" -> return $ return $ encJFromJValue mutationRootName
      _            -> fmap liftTx . evalReusabilityT $ GR.mutFldToTx fld
    return (G.unName $ G.unAlias $ VQ._fAlias fld, fldRespTx)

  -- combines all transactions into a single transaction
  return $ toSingleTx aliasedTxs
  where
    -- A list of aliased transactions for eg
    -- [("f1", Tx r1), ("f2", Tx r2)]
    -- are converted into a single transaction as follows
    -- Tx {"f1": r1, "f2": r2}
    toSingleTx :: [(Text, LazyRespTx)] -> LazyRespTx
    toSingleTx aliasedTxs =
      fmap encJFromAssocList $
      forM aliasedTxs $ \(al, tx) -> (,) al <$> tx

getMutOp
  :: (MonadError QErr m)
  => GCtx
  -> SQLGenCtx
  -> UserInfo
  -> VQ.SelSet
  -> m LazyRespTx
getMutOp ctx sqlGenCtx userInfo selSet =
  runE ctx sqlGenCtx userInfo $ resolveMutSelSet selSet

getSubsOpM
  :: ( MonadError QErr m
     , MonadReader r m
     , Has QueryCtxMap r
     , Has FieldMap r
     , Has OrdByCtx r
     , Has SQLGenCtx r
     , Has UserInfo r
     , MonadIO m
     )
  => IsPGExecCtx
  -> QueryReusability
  -> VQ.Field
  -> m (EL.LiveQueryPlan, Maybe EL.ReusableLiveQueryPlan)
getSubsOpM isPgCtx initialReusability fld =
  case VQ._fName fld of
    "__typename" ->
      throwVE "you cannot create a subscription on '__typename' field"
    _            -> do
      (astUnresolved, finalReusability) <- runReusabilityTWith initialReusability $
        GR.queryFldToPGAST fld
      let varTypes = finalReusability ^? _Reusable
      EL.buildLiveQueryPlan isPgCtx (VQ._fAlias fld) astUnresolved varTypes

getSubsOp
  :: ( MonadError QErr m
     , MonadIO m
     )
  => IsPGExecCtx
  -> GCtx
  -> SQLGenCtx
  -> UserInfo
  -> QueryReusability
  -> VQ.Field
  -> m (EL.LiveQueryPlan, Maybe EL.ReusableLiveQueryPlan)
getSubsOp isPgCtx gCtx sqlGenCtx userInfo queryReusability fld =
  runE gCtx sqlGenCtx userInfo $ getSubsOpM isPgCtx queryReusability fld

execRemoteGQ
  :: ( HasVersion
     , MonadIO m
     , MonadError QErr m
     , MonadReader ExecutionCtx m
     )
  => RequestId
  -> UserInfo
  -> [HTTP.Header]
  -> GQLReqUnparsed
  -> RemoteSchemaInfo
  -> G.TypedOperationDefinition
  -> m (DiffTime, HttpResponse EncJSON)
  -- ^ Also returns time spent in http request, for telemetry.
execRemoteGQ reqId userInfo reqHdrs q rsi opDef = do
  execCtx <- ask
  let logger  = _ecxLogger execCtx
      manager = _ecxHttpManager execCtx
      opTy    = G._todType opDef
  when (opTy == G.OperationTypeSubscription) $
    throw400 NotSupported "subscription to remote server is not supported"
  hdrs <- getHeadersFromConf hdrConf
  let confHdrs   = map (\(k, v) -> (CI.mk $ CS.cs k, CS.cs v)) hdrs
      clientHdrs = bool [] filteredHeaders fwdClientHdrs
      -- filter out duplicate headers
      -- priority: conf headers > resolved userinfo vars > x-forwarded headers > client headers
      hdrMaps    = [ Map.fromList confHdrs
                   , Map.fromList userInfoToHdrs
                   , Map.fromList xForwardedHeaders
                   , Map.fromList clientHdrs
                   ]
      headers  = Map.toList $ foldr Map.union Map.empty hdrMaps
      finalHeaders = addDefaultHeaders headers
  initReqE <- liftIO $ try $ HTTP.parseRequest (show url)
  initReq <- either httpThrow pure initReqE
  let req = initReq
           { HTTP.method = "POST"
           , HTTP.requestHeaders = finalHeaders
           , HTTP.requestBody = HTTP.RequestBodyLBS (J.encode q)
           , HTTP.responseTimeout = HTTP.responseTimeoutMicro (timeout * 1000000)
           }

  L.unLogger logger $ QueryLog q Nothing reqId
  (time, res)  <- withElapsedTime $ liftIO $ try $ HTTP.httpLbs req manager
  resp <- either httpThrow return res
  let cookieHdrs = getCookieHdr (resp ^.. Wreq.responseHeader "Set-Cookie")
      respHdrs  = Just $ mkRespHeaders cookieHdrs
      !httpResp = HttpResponse (encJFromLBS $ resp ^. Wreq.responseBody) respHdrs
  return (time, httpResp)

  where
    RemoteSchemaInfo url hdrConf fwdClientHdrs timeout = rsi
    httpThrow :: (MonadError QErr m) => HTTP.HttpException -> m a
    httpThrow = \case
      HTTP.HttpExceptionRequest _req content -> throw500 $ T.pack . show $ content
      HTTP.InvalidUrlException _url reason -> throw500 $ T.pack . show $ reason

    userInfoToHdrs = map (\(k, v) -> (CI.mk $ CS.cs k, CS.cs v)) $
                     userInfoToList userInfo
    filteredHeaders = filterUserVars $ filterRequestHeaders reqHdrs

    xForwardedHeaders = flip mapMaybe reqHdrs $ \(hdrName, hdrValue) ->
      case hdrName of
        "Host"       -> Just ("X-Forwarded-Host", hdrValue)
        "User-Agent" -> Just ("X-Forwarded-User-Agent", hdrValue)
        _            -> Nothing

    filterUserVars hdrs =
      let txHdrs = map (\(n, v) -> (bsToTxt $ CI.original n, bsToTxt v)) hdrs
      in map (\(k, v) -> (CI.mk $ CS.cs k, CS.cs v)) $
         filter (not . isUserVar . fst) txHdrs

    getCookieHdr = fmap (\h -> ("Set-Cookie", h))

    mkRespHeaders = map (\(k, v) -> Header (bsToTxt $ CI.original k, bsToTxt v))
