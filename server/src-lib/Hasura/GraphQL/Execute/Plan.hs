module Hasura.GraphQL.Execute.Plan
  ( ReusablePlan(..)
  , PlanCache
  , PlanCacheOptions(..)
  , getPlan
  , addPlan
  , initPlanCache
  , clearPlanCache
  , dumpPlanCache
  ) where

import           Hasura.Prelude
import           Hasura.Session

import qualified Data.Aeson                             as J
import qualified Data.Aeson.Casing                      as J
import qualified Data.Aeson.TH                          as J
import qualified Database.PG.Query                      as Q

import qualified Hasura.Cache                           as Cache
import qualified Hasura.GraphQL.Execute.LiveQuery       as LQ
import qualified Hasura.GraphQL.Execute.Query           as EQ
import qualified Hasura.GraphQL.Resolve                 as R
import qualified Hasura.GraphQL.Transport.HTTP.Protocol as GH
import           Hasura.RQL.Types

data PlanId
  = PlanId
  { _piSchemaCacheVersion :: !SchemaCacheVer
  , _piRole               :: !RoleName
  , _piOperationName      :: !(Maybe GH.OperationName)
  , _piQuery              :: !GH.GQLQueryText
  , _piQueryType          :: !EQ.GraphQLQueryType
  } deriving (Show, Eq, Ord, Generic)

instance Hashable PlanId

instance J.ToJSON PlanId where
  toJSON (PlanId scVer rn opNameM query queryType) =
    J.object
    [ "schema_cache_version" J..= scVer
    , "role" J..= rn
    , "operation" J..= opNameM
    , "query" J..= query
    , "query_type" J..= queryType
    ]

newtype PlanCache
  = PlanCache {_unPlanCache :: Cache.Cache PlanId ReusablePlan}

data ReusablePlan
  = RPQuery !EQ.ReusableQueryPlan !Q.TxAccess [R.QueryRootFldUnresolved]
  | RPSubs !LQ.ReusableLiveQueryPlan !Q.TxAccess

instance J.ToJSON ReusablePlan where
  toJSON = \case
    RPQuery queryPlan _ _ -> J.toJSON queryPlan
    RPSubs subsPlan _ -> J.toJSON subsPlan

newtype PlanCacheOptions
  = PlanCacheOptions { unPlanCacheSize :: Maybe Cache.CacheSize }
  deriving (Show, Eq)
$(J.deriveJSON (J.aesonDrop 2 J.snakeCase) ''PlanCacheOptions)

initPlanCache :: PlanCacheOptions -> IO PlanCache
initPlanCache options =
  PlanCache <$> Cache.initialise (unPlanCacheSize options)

getPlan
  :: SchemaCacheVer -> RoleName -> Maybe GH.OperationName -> GH.GQLQueryText
  -> EQ.GraphQLQueryType -> PlanCache -> IO (Maybe ReusablePlan)
getPlan schemaVer rn opNameM q queryType (PlanCache planCache) =
  Cache.lookup planId planCache
  where
    planId = PlanId schemaVer rn opNameM q queryType

addPlan
  :: SchemaCacheVer -> RoleName -> Maybe GH.OperationName -> GH.GQLQueryText
  -> ReusablePlan -> EQ.GraphQLQueryType -> PlanCache -> IO ()
addPlan schemaVer rn opNameM q queryPlan queryType (PlanCache planCache) =
  Cache.insert planId queryPlan planCache
  where
    planId = PlanId schemaVer rn opNameM q queryType

clearPlanCache :: PlanCache -> IO ()
clearPlanCache (PlanCache planCache) =
  Cache.clear planCache

dumpPlanCache :: PlanCache -> IO J.Value
dumpPlanCache (PlanCache cache) =
  J.toJSON . map (map dumpEntry) <$> Cache.getEntries cache
  where
    dumpEntry (planId, plan) =
      J.object
      [ "id" J..= planId
      , "plan" J..= plan
      ]
