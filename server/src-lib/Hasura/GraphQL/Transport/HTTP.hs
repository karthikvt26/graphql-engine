{-# LANGUAGE RecordWildCards #-}
module Hasura.GraphQL.Transport.HTTP
  ( runGQ
  , runGQBatched
  , GQLReq(..)
  , GQLReqUnparsed
  , GQLReqParsed
  , GQLExecDoc(..)
  , OperationName(..)
  , GQLQueryText(..)
  ) where

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

import qualified Database.PG.Query                      as Q
import qualified Hasura.GraphQL.Execute                 as E
import qualified Hasura.Server.Telemetry.Counters       as Telem
import qualified Language.GraphQL.Draft.Syntax          as G
import qualified Network.HTTP.Types                     as HTTP

runGQ
  :: ( HasVersion
     , MonadIO m
     , MonadError QErr m
     , MonadReader E.ExecutionCtx m
     , QueryLogger m
     )
  => RequestId
  -> UserInfo
  -> [HTTP.Header]
  -> (GQLReqUnparsed, GQLReqParsed)
  -> m (HttpResponse EncJSON)
runGQ reqId userInfo reqHdrs req@(reqUnparsed, _) = do
  -- The response and misc telemetry data:
  let telemTransport = Telem.HTTP
  (telemTimeTot_DT, (telemCacheHit, telemLocality, (telemTimeIO_DT, telemQueryType, !resp))) <- withElapsedTime $ do
    E.ExecutionCtx _ sqlGenCtx pgExecCtx planCache sc scVer httpManager enableAL <- ask
    (telemCacheHit, execPlan) <- E.getResolvedExecPlan pgExecCtx planCache
                                 userInfo sqlGenCtx enableAL sc scVer httpManager reqHdrs req
    case execPlan of
      E.GExPHasura (resolvedOp, txAccess) -> do
        (telemTimeIO, telemQueryType, respHdrs, resp) <- runHasuraGQ reqId reqUnparsed userInfo txAccess resolvedOp
        return (telemCacheHit, Telem.Local, (telemTimeIO, telemQueryType, HttpResponse resp respHdrs))
      E.GExPRemote rsi opDef  -> do
        let telemQueryType | G._todType opDef == G.OperationTypeMutation = Telem.Mutation
                           | otherwise = Telem.Query
        (telemTimeIO, resp) <- E.execRemoteGQ reqId userInfo reqHdrs reqUnparsed rsi opDef
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
     , QueryLogger m
     , E.GQLApiAuthorization m
     )
  => RequestId
  -> ResponseInternalErrorsConfig
  -> UserInfo
  -> IpAddress
  -> [HTTP.Header]
  -> GQLBatchedReqs GQLQueryText
  -> m (HttpResponse EncJSON)
runGQBatched reqId responseErrorsConfig userInfo ipAddress reqHdrs query = do
  schemaCache <- asks E._ecxSchemaCache
  enableAL <- asks E._ecxEnableAllowList
  case query of
    GQLSingleRequest req -> do
      -- run system authorization on the GraphQL API
      reqParsed <- E.authorizeGQLApi userInfo (reqHdrs, ipAddress) enableAL schemaCache req
                   >>= flip onLeft throwError
      -- then run the query
      runGQ reqId userInfo reqHdrs (req, reqParsed)
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
        traverse (try . runGQ reqId userInfo reqHdrs) $ zip reqs reqsParsed
  where
    try = flip catchError (pure . Left) . fmap Right

runHasuraGQ
  :: ( MonadIO m
     , MonadError QErr m
     , MonadReader E.ExecutionCtx m
     , QueryLogger m
     )
  => RequestId
  -> GQLReqUnparsed
  -> UserInfo
  -> Q.TxAccess
  -> E.ExecOp
  -> m (DiffTime, Telem.QueryType, HTTP.ResponseHeaders, EncJSON)
  -- ^ Also return 'Mutation' when the operation was a mutation, and the time
  -- spent in the PG query; for telemetry.
runHasuraGQ reqId query userInfo txAccess resolvedOp = do
  E.ExecutionCtx logger _ isPgCtx _ _ _ _ _ <- ask
  logQuery' logger
  (telemTimeIO, respE) <- withElapsedTime $ runExceptT $ executeTx isPgCtx
  (respHdrs, resp)     <- liftEither respE

  let !json          = encodeGQResp $ GQSuccess $ encJToLBS resp
      telemQueryType = case resolvedOp of E.ExOpMutation{} -> Telem.Mutation ; _ -> Telem.Query
  return (telemTimeIO, telemQueryType, respHdrs, json)
  where
    runLazyTx' = case txAccess of
      Q.ReadOnly  -> runLazyROTx'
      Q.ReadWrite -> runLazyRWTx'

    executeTx isPgCtx = case resolvedOp of
      E.ExOpQuery tx _    ->
        ([],) <$> runLazyTx' isPgCtx tx
      E.ExOpMutation respHeaders tx -> do
        (respHeaders,) <$> runLazyTx Q.ReadWrite isPgCtx (withUserInfo userInfo tx)
      E.ExOpSubs _ ->
        throw400 UnexpectedPayload
        "subscriptions are not supported over HTTP, use websockets instead"

    logQuery' logger = case resolvedOp of
      -- log the generated SQL and the graphql query
      E.ExOpQuery _ genSql -> logQuery logger query genSql reqId
      -- log the graphql query
      E.ExOpMutation _ _   -> logQuery logger query Nothing reqId
      E.ExOpSubs _         -> return ()
