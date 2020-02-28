{-# LANGUAGE DataKinds       #-}
{-# LANGUAGE RecordWildCards #-}

module Hasura.GraphQL.Execute
  ( GQExecPlanPartial
  , GQExecPlan(..)

  , getExecPlanPartial

  , ExecOp(..)
  , getResolvedExecPlan
  , execRemoteGQ
  , execRemoteGQ'
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

import           Control.Lens
import           Data.Has
import           Data.Time

import qualified Data.Aeson                             as J
import qualified Data.HashMap.Strict                    as Map
import qualified Data.HashSet                           as Set
import qualified Language.GraphQL.Draft.Syntax          as G
import qualified Network.HTTP.Client                    as HTTP
import qualified Network.HTTP.Types                     as HTTP

import           Hasura.EncJSON
import           Hasura.GraphQL.Context
import           Hasura.GraphQL.Logging
import           Hasura.GraphQL.RemoteServer            (execRemoteGQ')
import           Hasura.GraphQL.Resolve.Context
import           Hasura.GraphQL.Schema
import           Hasura.GraphQL.Transport.HTTP.Protocol
import           Hasura.GraphQL.Validate.Types
import           Hasura.Prelude
import           Hasura.RQL.Types
import           Hasura.Server.Context
import           Hasura.Server.Utils                    (IpAddress, RequestId)
import           Hasura.Server.Version                  (HasVersion)

import qualified Hasura.GraphQL.Execute.LiveQuery       as EL
import qualified Hasura.GraphQL.Execute.Plan            as EP
import qualified Hasura.GraphQL.Execute.Query           as EQ
import qualified Hasura.GraphQL.Resolve                 as GR
import qualified Hasura.GraphQL.Validate                as VQ
import qualified Hasura.GraphQL.Validate.Types          as VT
import qualified Hasura.Logging                         as L
import qualified Hasura.Server.Telemetry.Counters       as Telem

-- | Execution context
data ExecutionCtx
  = ExecutionCtx
  { _ecxLogger          :: !(L.Logger L.Hasura)
  , _ecxSqlGenCtx       :: !SQLGenCtx
  , _ecxPgExecCtx       :: !PGExecCtx
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

data GQExecPlan a
  = GExPHasura !a
  | GExPRemote !RemoteSchemaInfo !G.TypedOperationDefinition
  deriving (Show, Eq, Functor, Foldable, Traversable)

type GQExecPlanPartial = GQExecPlan (GCtx, VQ.RootSelSet)

getExecPlanPartial
  :: (MonadReusability m, MonadError QErr m)
  => UserInfo
  -> SchemaCache
  -> Bool
  -> GQLReqParsed
  -> m GQExecPlanPartial
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
      pure $ GExPHasura (gCtx, rootSelSet)
    VT.TLRemoteType _ rsi  ->
      pure $ GExPRemote rsi opDef
    VT.TLCustom ->
      throw500 "unexpected custom type for top level field"
  where
    role = userRole userInfo
    gCtxRoleMap = scGCtxMap sc
    checkQueryInAllowlist =
      when (role /= adminRole) $ do
        let notInAllowlist =
              not $ VQ.isQueryInAllowlist (_grQuery req) (scAllowlist sc)
        when notInAllowlist $ modifyQErr modErr $ throwVE "query is not allowed"

    modErr e =
      let msg = "query is not in any of the allowlists"
       in e {qeInternal = Just $ J.object ["message" J..= J.String msg]}


-- transaction to be executed
data ExecOp
  = ExOpQuery !LazyRespTx !(Maybe EQ.GeneratedSqlMap)
  | ExOpMutation !LazyRespTx
  | ExOpSubs !EL.LiveQueryPlan
  deriving Show

type GQExecPlanResolved = GQExecPlan ExecOp

getResolvedExecPlan
  :: forall m. (HasVersion, MonadError QErr m, MonadIO m)
  => PGExecCtx
  -> EP.PlanCache
  -> UserInfo
  -> SQLGenCtx
  -> Bool
  -> SchemaCache
  -> SchemaCacheVer
  -> HTTP.Manager
  -> [HTTP.Header]
  -> (GQLReqUnparsed, GQLReqParsed)
  -> m (Telem.CacheHit, GQExecPlanResolved)
getResolvedExecPlan pgExecCtx planCache userInfo sqlGenCtx
  enableAL sc scVer httpManager reqHeaders (reqUnparsed, reqParsed) = do

  planM <- liftIO $ EP.getPlan scVer (userRole userInfo)
           opNameM queryStr planCache
  let usrVars = userVars userInfo
  case planM of
    -- plans are only for queries and subscriptions
    Just plan -> (Telem.Hit,) . GExPHasura <$> case plan of
      EP.RPQuery queryPlan -> do
        (tx, genSql) <- EQ.queryOpFromPlan httpManager reqHeaders userInfo queryVars queryPlan
        pure $ ExOpQuery tx (Just genSql)
      EP.RPSubs subsPlan ->
        ExOpSubs <$> EL.reuseLiveQueryPlan pgExecCtx usrVars queryVars subsPlan
    Nothing -> (Telem.Miss,) <$> noExistingPlan
  where
    GQLReq opNameM queryStr queryVars = reqUnparsed
    addPlanToCache plan = liftIO $ EP.addPlan scVer (userRole userInfo) opNameM queryStr plan planCache

    noExistingPlan :: m GQExecPlanResolved
    noExistingPlan = do
      (partialExecPlan, queryReusability) <- runReusabilityT $
        getExecPlanPartial userInfo sc enableAL reqParsed
      forM partialExecPlan $ \(gCtx, rootSelSet) ->
        case rootSelSet of
          VQ.RMutation selSet ->
            ExOpMutation <$> getMutOp gCtx sqlGenCtx userInfo httpManager reqHeaders selSet
          VQ.RQuery selSet -> do
              (queryTx, plan, genSql) <- getQueryOp gCtx sqlGenCtx httpManager reqHeaders userInfo queryReusability selSet
              traverse_ (addPlanToCache . EP.RPQuery) plan
              pure $ ExOpQuery queryTx (Just genSql)
          VQ.RSubscription fld -> do
            (lqOp, plan) <- getSubsOp pgExecCtx gCtx sqlGenCtx userInfo queryReusability fld
            traverse_ (addPlanToCache . EP.RPSubs) plan
            pure $ ExOpSubs lqOp

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
  :: (HasVersion, MonadError QErr m)
  => GCtx
  -> SQLGenCtx
  -> HTTP.Manager
  -> [HTTP.Header]
  -> UserInfo
  -> QueryReusability
  -> VQ.SelSet
  -> m (LazyRespTx, Maybe EQ.ReusableQueryPlan, EQ.GeneratedSqlMap)
getQueryOp gCtx sqlGenCtx manager reqHdrs userInfo queryReusability fields =
  runE gCtx sqlGenCtx userInfo $ EQ.convertQuerySelSet manager reqHdrs queryReusability fields

mutationRootName :: Text
mutationRootName = "mutation_root"

resolveMutSelSet
  :: ( HasVersion
     , MonadError QErr m
     , MonadReader r m
     , Has UserInfo r
     , Has MutationCtxMap r
     , Has FieldMap r
     , Has OrdByCtx r
     , Has SQLGenCtx r
     , Has InsCtxMap r
     , Has HTTP.Manager r
     , Has [HTTP.Header] r
     , MonadIO m
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
  return $ liftTx $ toSingleTx aliasedTxs
  where
    toSingleTx aliasedTxs = fmap encJFromAssocList $ forM aliasedTxs sequence

getMutOp
  :: (HasVersion, MonadError QErr m, MonadIO m)
  => GCtx
  -> SQLGenCtx
  -> UserInfo
  -> HTTP.Manager
  -> [HTTP.Header]
  -> VQ.SelSet
  -> m LazyRespTx
getMutOp ctx sqlGenCtx userInfo manager reqHeaders selSet =
  runE_ $ resolveMutSelSet selSet
  where
    runE_ action = do
      res <- runExceptT $ runReaderT action
        ( userInfo, queryCtxMap, mutationCtxMap
        , typeMap, fldMap, ordByCtx, insCtxMap, sqlGenCtx
        , manager, reqHeaders
        )
      either throwError return res
      where
        queryCtxMap = _gQueryCtxMap ctx
        mutationCtxMap = _gMutationCtxMap ctx
        typeMap = _gTypes ctx
        fldMap = _gFields ctx
        ordByCtx = _gOrdByCtx ctx
        insCtxMap = _gInsCtxMap ctx

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
  => PGExecCtx
  -> QueryReusability
  -> VQ.Field
  -> m (EL.LiveQueryPlan, Maybe EL.ReusableLiveQueryPlan)
getSubsOpM pgExecCtx initialReusability fld =
  case VQ._fName fld of
    "__typename" ->
      throwVE "you cannot create a subscription on '__typename' field"
    _            -> do
      (astUnresolved, finalReusability) <- runReusabilityTWith initialReusability $
        GR.queryFldToPGAST fld
      let varTypes = finalReusability ^? _Reusable
      EL.buildLiveQueryPlan pgExecCtx (VQ._fAlias fld) astUnresolved varTypes

getSubsOp
  :: ( MonadError QErr m
     , MonadIO m
     )
  => PGExecCtx
  -> GCtx
  -> SQLGenCtx
  -> UserInfo
  -> QueryReusability
  -> VQ.Field
  -> m (EL.LiveQueryPlan, Maybe EL.ReusableLiveQueryPlan)
getSubsOp pgExecCtx gCtx sqlGenCtx userInfo queryReusability fld =
  runE gCtx sqlGenCtx userInfo $ getSubsOpM pgExecCtx queryReusability fld

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
  -> G.OperationType
  -> m (DiffTime, HttpResponse EncJSON)
  -- ^ Also returns time spent in http request, for telemetry.
execRemoteGQ reqId userInfo reqHdrs q rsi opType = do
  execCtx <- ask
  let logger  = _ecxLogger execCtx
      manager = _ecxHttpManager execCtx
  L.unLogger logger $ QueryLog q Nothing reqId
  (time, respHdrs, resp) <- execRemoteGQ' manager userInfo reqHdrs q rsi opType
  let !httpResp = HttpResponse (encJFromLBS resp) respHdrs
  return (time, httpResp)
