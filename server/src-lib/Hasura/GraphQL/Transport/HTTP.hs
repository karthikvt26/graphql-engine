{-# LANGUAGE RecordWildCards #-}
module Hasura.GraphQL.Transport.HTTP
  ( MonadExecuteQuery(..)
  , runGQ
  , runGQBatched
  , GQLReq(..)
  , GQLReqUnparsed
  , GQLReqParsed
  , GQLExecDoc(..)
  , OperationName(..)
  , GQLQueryText(..)
  ) where

import           Control.Monad.Morph (hoist)

import           Hasura.EncJSON
import           Hasura.GraphQL.Transport.HTTP.Protocol
import           Hasura.HTTP
import           Hasura.Prelude
import           Hasura.RQL.Types
import           Hasura.Server.Init.Config
import           Hasura.Server.Logging                  (QueryLogger (..))
import           Hasura.Server.Utils                    (IpAddress, RequestId)
import           Hasura.Server.Version                  (HasVersion)
import           Hasura.Session
import           Hasura.Tracing                         (MonadTrace, TraceT, trace)

import qualified Data.Environment                       as Env
import qualified Database.PG.Query                      as Q
import qualified Hasura.GraphQL.Execute                 as E
import qualified Hasura.GraphQL.Execute.Query           as EQ
import qualified Hasura.GraphQL.Resolve                 as R
import qualified Hasura.Server.Telemetry.Counters       as Telem
import qualified Language.GraphQL.Draft.Syntax          as G
import qualified Network.HTTP.Types                     as HTTP

class Monad m => MonadExecuteQuery m where
  executeQuery 
    :: GQLReqParsed
    -> [R.QueryRootFldUnresolved]
    -> Maybe EQ.GeneratedSqlMap
    -> IsPGExecCtx
    -> Q.TxAccess
    -> LazyTx QErr EncJSON
    -> ExceptT QErr m (HTTP.ResponseHeaders, EncJSON)

instance MonadExecuteQuery m => MonadExecuteQuery (ReaderT r m) where
  executeQuery a b c d e f = hoist lift $ executeQuery a b c d e f
  
instance MonadExecuteQuery m => MonadExecuteQuery (ExceptT r m) where
  executeQuery a b c d e f = hoist lift $ executeQuery a b c d e f

instance MonadExecuteQuery m => MonadExecuteQuery (TraceT m) where
  executeQuery a b c d e f = hoist lift $ executeQuery a b c d e f

runGQ
  :: ( HasVersion
     , MonadIO m
     , MonadError QErr m
     , MonadReader E.ExecutionCtx m
     , MonadExecuteQuery m
     , QueryLogger m
     , MonadTrace m
     )
  => Env.Environment
  -> RequestId
  -> UserInfo
  -> [HTTP.Header]
  -> (GQLReqUnparsed, GQLReqParsed)
  -> m (HttpResponse EncJSON)
runGQ env reqId userInfo reqHdrs req@(reqUnparsed, _) = do
  -- The response and misc telemetry data:
  let telemTransport = Telem.HTTP
  (telemTimeTot_DT, (telemCacheHit, telemLocality, (telemTimeIO_DT, telemQueryType, !resp))) <- withElapsedTime $ do
    E.ExecutionCtx _ sqlGenCtx pgExecCtx planCache sc scVer httpManager _ <- ask
    (telemCacheHit, execPlan) <- E.getResolvedExecPlan env pgExecCtx planCache
                                 userInfo sqlGenCtx sc scVer httpManager reqHdrs req
    case execPlan of
      E.GExPHasura (resolvedOp, txAccess) -> do
        (telemTimeIO, telemQueryType, respHdrs, resp) <- runHasuraGQ reqId req userInfo txAccess resolvedOp
        return (telemCacheHit, Telem.Local, (telemTimeIO, telemQueryType, HttpResponse resp respHdrs))
      E.GExPRemote rsi opDef  -> do
        let telemQueryType | G._todType opDef == G.OperationTypeMutation = Telem.Mutation
                           | otherwise = Telem.Query
        (telemTimeIO, resp) <- E.execRemoteGQ env reqId userInfo reqHdrs reqUnparsed rsi opDef
        return (telemCacheHit, Telem.Remote, (telemTimeIO, telemQueryType, resp))
  let telemTimeIO = fromUnits telemTimeIO_DT
      telemTimeTot = fromUnits telemTimeTot_DT
  Telem.recordTimingMetric Telem.RequestDimensions{..} Telem.RequestTimings{..}
  return resp

runGQBatched
  :: ( HasVersion
     , MonadIO m
     , MonadError QErr m
     , MonadReader E.ExecutionCtx m
     , MonadExecuteQuery m
     , QueryLogger m
     , MonadTrace m
     , E.GQLApiAuthorization m
     )
  => Env.Environment
  -> RequestId
  -> ResponseInternalErrorsConfig
  -> UserInfo
  -> IpAddress
  -> [HTTP.Header]
  -> GQLBatchedReqs GQLQueryText
  -> m (HttpResponse EncJSON)
runGQBatched env reqId responseErrorsConfig userInfo ipAddress reqHdrs query = do
  schemaCache <- asks E._ecxSchemaCache
  enableAL <- asks E._ecxEnableAllowList
  case query of
    GQLSingleRequest req -> do
      -- run system authorization on the GraphQL API
      reqParsed <- E.authorizeGQLApi userInfo (reqHdrs, ipAddress) enableAL schemaCache req
                   >>= flip onLeft throwError
      -- then run the query
      runGQ env reqId userInfo reqHdrs (req, reqParsed)
    GQLBatchedReqs reqs -> do
      -- It's unclear what we should do if we receive multiple
      -- responses with distinct headers, so just do the simplest thing
      -- in this case, and don't forward any.
      let includeInternal = shouldIncludeInternal (_uiRole userInfo) responseErrorsConfig
          removeHeaders =
            flip HttpResponse []
            . encJFromList
            . map (either (encJFromJValue . encodeGQErr includeInternal) _hrBody)
      -- run system authorization on the GraphQL API
      reqsParsed <- traverse (E.authorizeGQLApi userInfo (reqHdrs, ipAddress) enableAL schemaCache) reqs
                    >>= mapM (flip onLeft throwError)
      -- then run the query
      fmap removeHeaders $
        traverse (try . runGQ env reqId userInfo reqHdrs) $ zip reqs reqsParsed
  where
    try = flip catchError (pure . Left) . fmap Right

runHasuraGQ
  :: ( MonadIO m
     , MonadError QErr m
     , MonadReader E.ExecutionCtx m
     , MonadExecuteQuery m
     , QueryLogger m
     , MonadTrace m
     )
  => RequestId
  -> (GQLReqUnparsed, GQLReqParsed)
  -> UserInfo
  -> Q.TxAccess
  -> E.ExecOp
  -> m (DiffTime, Telem.QueryType, HTTP.ResponseHeaders, EncJSON)
  -- ^ Also return 'Mutation' when the operation was a mutation, and the time
  -- spent in the PG query; for telemetry.
runHasuraGQ reqId (query, queryParsed) userInfo txAccess resolvedOp = do
  E.ExecutionCtx logger _ isPgCtx _ _ _ _ _ <- ask
  logQuery' logger
  (telemTimeIO, respE) <- withElapsedTime . runExceptT $ executeTx isPgCtx
  (respHdrs, resp) <- liftEither respE

  let !json          = encodeGQResp $ GQSuccess $ encJToLBS resp
      telemQueryType = case resolvedOp of E.ExOpMutation{} -> Telem.Mutation ; _ -> Telem.Query
  return (telemTimeIO, telemQueryType, respHdrs, json)
  where
    executeTx isPgCtx =
      case resolvedOp of
        E.ExOpQuery tx genSql asts -> trace "pg" $ do
          executeQuery queryParsed asts genSql isPgCtx txAccess tx
        E.ExOpMutation respHeaders tx -> trace "pg" $ do
          (respHeaders,) <$> runLazyTx Q.ReadWrite isPgCtx (withUserInfo userInfo tx)
        E.ExOpSubs _ ->
          throw400 UnexpectedPayload
            "subscriptions are not supported over HTTP, use websockets instead"

    logQuery' logger = case resolvedOp of
      -- log the generated SQL and the graphql query
      E.ExOpQuery _ genSql _ -> logQuery logger query genSql reqId
      -- log the graphql query
      E.ExOpMutation _ _   -> logQuery logger query Nothing reqId
      E.ExOpSubs _         -> return ()