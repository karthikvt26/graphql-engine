module Hasura.GraphQL.Transport.HTTP
  ( runGQ
  , GQLReq(..)
  , GQLReqUnparsed
  , GQLReqParsed
  , GQLExecDoc(..)
  , OperationName(..)
  , GQLQueryText(..)
  , GQLApiAuthorization (..)
  ) where

import qualified Network.HTTP.Types                     as N

import           Hasura.EncJSON
import           Hasura.GraphQL.Logging
import           Hasura.GraphQL.Transport.HTTP.Protocol
import           Hasura.Prelude
import           Hasura.RQL.Types
import           Hasura.Server.Context
import           Hasura.Server.Utils                    (IpAddress, RequestId)

import qualified Database.PG.Query                      as Q
import qualified Hasura.GraphQL.Execute                 as E
import qualified Hasura.Logging                         as L

-- | Typeclass representing the GraphQL API (over both HTTP & Websockets) authorization effect
class Monad m => GQLApiAuthorization m where
  authorizeGQLApi
    :: UserInfo
    -> ([N.Header], IpAddress)
    -- ^ request headers and IP address
    -> GQLReqUnparsed
    -- ^ the unparsed GraphQL query string and related object
    -> m (Either QErr GQLReqParsed)

runGQ
  :: ( MonadIO m
     , MonadError QErr m
     , MonadReader E.ExecutionCtx m
     )
  => RequestId
  -> UserInfo
  -> [N.Header]
  -> (GQLReqUnparsed, GQLReqParsed)
  -> m (HttpResponse EncJSON)
runGQ reqId userInfo reqHdrs req@(reqUnparsed, _) = do
  E.ExecutionCtx _ sqlGenCtx pgExecCtx planCache sc scVer _ enableAL <- ask
  execPlan <- E.getResolvedExecPlan pgExecCtx planCache userInfo sqlGenCtx enableAL sc scVer req
  case execPlan of
    E.GExPHasura resolvedOp ->
      flip HttpResponse Nothing <$> runHasuraGQ reqId reqUnparsed userInfo resolvedOp
    E.GExPRemote rsi opDef  ->
      E.execRemoteGQ reqId userInfo reqHdrs reqUnparsed rsi opDef

runHasuraGQ
  :: ( MonadIO m
     , MonadError QErr m
     , MonadReader E.ExecutionCtx m
     )
  => RequestId
  -> GQLReqUnparsed
  -> UserInfo
  -> E.ExecOp
  -> m EncJSON
runHasuraGQ reqId query userInfo resolvedOp = do
  E.ExecutionCtx logger _ pgExecCtx _ _ _ _ _ <- ask
  respE <- liftIO $ runExceptT $ case resolvedOp of
    E.ExOpQuery tx genSql  -> do
      -- log the generated SQL and the graphql query
      L.unLogger logger $ QueryLog query genSql reqId
      runLazyTx' pgExecCtx tx
    E.ExOpMutation tx -> do
      -- log the graphql query
      L.unLogger logger $ QueryLog query Nothing reqId
      runLazyTx pgExecCtx Q.ReadWrite $ withUserInfo userInfo tx
    E.ExOpSubs _ ->
      throw400 UnexpectedPayload
      "subscriptions are not supported over HTTP, use websockets instead"
  resp <- liftEither respE
  return $ encodeGQResp $ GQSuccess $ encJToLBS resp
