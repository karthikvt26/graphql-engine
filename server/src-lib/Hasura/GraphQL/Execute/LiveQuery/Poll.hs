-- | Multiplexed live query poller threads; see "Hasura.GraphQL.Execute.LiveQuery" for details.
module Hasura.GraphQL.Execute.LiveQuery.Poll (
  -- * Pollers
    Poller(..)
  , PollerId(..)
  , PollerIOState(..)
  , pollQuery

  , PollerKey(..)
  , PollerMap
  , dumpPollerMap

  , RefetchMetrics
  , initRefetchMetrics

  -- * Cohorts
  , Cohort(..)
  , CohortId
  , newCohortId
  , CohortVariables
  , CohortKey
  , CohortMap

  -- * Subscribers
  , Subscriber(..)
  , SubscriberId
  , UniqueSubscriberId(..)
  , newSinkId
  , SubscriberMap
  , OnChange
  , LGQResponse
  , LiveQueryResponse(..)
  , LiveQueryMetadata(..)
  ) where

import           Hasura.Prelude

import qualified Control.Concurrent.Async                    as A
import qualified Control.Concurrent.STM                      as STM
import qualified Crypto.Hash                                 as CH
import qualified Data.Aeson.Casing                           as J
import qualified Data.Aeson.Extended                         as J
import qualified Data.Aeson.TH                               as J
import qualified Data.ByteString                             as BS
import qualified Data.ByteString.Lazy                        as BL
import qualified Data.HashMap.Strict                         as Map
import qualified Data.Time.Clock                             as Clock
import qualified Data.UUID                                   as UUID
import qualified Data.UUID.V4                                as UUID
import qualified Database.PG.Query                           as Q
import qualified Language.GraphQL.Draft.Syntax               as G
import qualified StmContainers.Map                           as STMMap
import qualified System.Metrics.Distribution                 as Metrics

import           Data.List.Split                             (chunksOf)

import qualified Hasura.GraphQL.Execute.LiveQuery.TMap       as TMap
import qualified Hasura.GraphQL.Transport.WebSocket.Protocol as WS
import qualified Hasura.GraphQL.Transport.WebSocket.Server   as WS
import qualified Hasura.Logging                              as L
import qualified ListT

import           Hasura.Db
import           Hasura.EncJSON
import           Hasura.GraphQL.Execute.LiveQuery.Options
import           Hasura.GraphQL.Execute.LiveQuery.Plan
import           Hasura.GraphQL.Transport.HTTP.Protocol
import           Hasura.RQL.Types

-- -------------------------------------------------------------------------------------------------
-- Subscribers

-- | This is to identify each subscriber based on their (websocket id, operation id) - this is
-- useful for collecting metrics and then correlating. The current 'SubscriberId' has a UUID which
-- is internal and not useful for metrics.
data UniqueSubscriberId
  = UniqueSubscriberId
  { _usiWebsocketId :: !WS.WSId
  , _usiOperationId :: !WS.OperationId
  } deriving (Show, Eq)

$(J.deriveToJSON (J.aesonDrop 4 J.snakeCase) ''UniqueSubscriberId)


data Subscriber
  = Subscriber
  { _sRootAlias            :: !G.Alias
  , _sOnChangeCallback     :: !OnChange
  , _sWebsocketOperationId :: !UniqueSubscriberId
  }

instance Show Subscriber where
  show (Subscriber root _ wsOpId) =
    "Subscriber { _sRootAlias = " <> show root <> ", _sOnChangeCallback = <fn> , _sWebsocketOperationId =" <> show wsOpId <> " }"

-- | live query onChange metadata, used for adding more extra analytics data
data LiveQueryMetadata
  = LiveQueryMetadata
  { _lqmExecutionTime :: !Clock.DiffTime
  }

data LiveQueryResponse
  = LiveQueryResponse
  { _lqrPayload       :: !BL.ByteString
  , _lqrExecutionTime :: !Clock.DiffTime
  }

type LGQResponse = GQResult LiveQueryResponse

type OnChange = LGQResponse -> IO ()

newtype SubscriberId = SubscriberId { _unSinkId :: UUID.UUID }
  deriving (Show, Eq, Hashable, J.ToJSON)

newSinkId :: IO SubscriberId
newSinkId = SubscriberId <$> UUID.nextRandom

type SubscriberMap = TMap.TMap SubscriberId Subscriber

-- -------------------------------------------------------------------------------------------------
-- Cohorts

-- | A batched group of 'Subscriber's who are not only listening to the same query but also have
-- identical session and query variables. Each result pushed to a 'Cohort' is forwarded along to
-- each of its 'Subscriber's.
--
-- In SQL, each 'Cohort' corresponds to a single row in the laterally-joined @_subs@ table (and
-- therefore a single row in the query result).
data Cohort
  = Cohort
  { _cCohortId            :: !CohortId
  -- ^ a unique identifier used to identify the cohort in the generated query
  , _cPreviousResponse    :: !(STM.TVar (Maybe ResponseHash))
  -- ^ a hash of the previous query result, if any, used to determine if we need to push an updated
  -- result to the subscribers or not
  , _cExistingSubscribers :: !SubscriberMap
  -- ^ the subscribers we’ve already pushed a result to; we push new results to them iff the
  -- response changes
  , _cNewSubscribers      :: !SubscriberMap
  -- ^ subscribers we haven’t yet pushed any results to; we push results to them regardless if the
  -- result changed, then merge them in the map of existing subscribers
  }

{- Note [Blake2b faster than SHA-256]
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
At the time of writing, from https://blake2.net, it is stated,
"BLAKE2 is a cryptographic hash function faster than MD5, SHA-1, SHA-2, and SHA-3,
yet is at least as secure as the latest standard SHA-3".
-}

-- | A hash used to determine if the result changed without having to keep the entire result in
-- memory. Using a cryptographic hash ensures that a hash collision is almost impossible: with 256
-- bits, even if a subscription changes once per second for an entire year, the probability of a
-- hash collision is ~4.294417×10-63. See Note [Blake2b faster than SHA-256].
newtype ResponseHash = ResponseHash { unResponseHash :: CH.Digest CH.Blake2b_256 }
  deriving (Show, Eq)

instance J.ToJSON ResponseHash where
  toJSON = J.toJSON . show . unResponseHash

mkRespHash :: BS.ByteString -> ResponseHash
mkRespHash = ResponseHash . CH.hash

-- | A key we use to determine if two 'Subscriber's belong in the same 'Cohort' (assuming they
-- already meet the criteria to be in the same 'Poller'). Note the distinction between this and
-- 'CohortId'; the latter is a completely synthetic key used only to identify the cohort in the
-- generated SQL query.
type CohortKey = CohortVariables
type CohortMap = TMap.TMap CohortKey Cohort

dumpCohortMap :: CohortMap -> IO J.Value
dumpCohortMap cohortMap = do
  cohorts <- STM.atomically $ TMap.toList cohortMap
  fmap J.toJSON . forM cohorts $ \(variableValues, cohort) -> do
    cohortJ <- dumpCohort cohort
    return $ J.object
      [ "variables" J..= variableValues
      , "cohort" J..= cohortJ
      ]
  where
    dumpCohort (Cohort respId respTV curOps newOps) =
      STM.atomically $ do
      prevResHash <- STM.readTVar respTV
      curOpIds <- TMap.toList curOps
      newOpIds <- TMap.toList newOps
      return $ J.object
        [ "resp_id" J..= respId
        , "current_ops" J..= map fst curOpIds
        , "new_ops" J..= map fst newOpIds
        , "previous_result_hash" J..= prevResHash
        ]

data CohortSnapshot
  = CohortSnapshot
  { _csVariables           :: !CohortVariables
  , _csPreviousResponse    :: !(STM.TVar (Maybe ResponseHash))
  , _csExistingSubscribers :: ![Subscriber]
  , _csNewSubscribers      :: ![Subscriber]
  }

pushResultToCohort
  :: GQResult EncJSON
  -- ^ a response that still needs to be wrapped with each 'Subscriber'’s root 'G.Alias'
  -> Maybe ResponseHash
  -> LiveQueryMetadata
  -> CohortSnapshot
  -> IO ()
pushResultToCohort result respHashM (LiveQueryMetadata dTime) cohortSnapshot = do
  prevRespHashM <- STM.readTVarIO respRef
  -- write to the current websockets if needed
  sinks <-
    if isExecError result || respHashM /= prevRespHashM
    then do
      STM.atomically $ STM.writeTVar respRef respHashM
      return (newSinks <> curSinks)
    else
      return newSinks
  pushResultToSubscribers sinks
  where
    CohortSnapshot _ respRef curSinks newSinks = cohortSnapshot
    pushResultToSubscribers = A.mapConcurrently_ $ \(Subscriber alias action _) ->
      let aliasText = G.unName $ G.unAlias alias
          wrapWithAlias response = LiveQueryResponse
            { _lqrPayload = encJToLBS $ encJFromAssocList [(aliasText, response)]
            , _lqrExecutionTime = dTime
            }
      in action (wrapWithAlias <$> result)

-- -------------------------------------------------------------------------------------------------
-- Pollers

-- | A unique, multiplexed query. Each 'Poller' has its own polling thread that periodically polls
-- Postgres and pushes results to each of its listening 'Cohort's.
--
-- In SQL, an 'Poller' corresponds to a single, multiplexed query, though in practice, 'Poller's
-- with large numbers of 'Cohort's are batched into multiple concurrent queries for performance
-- reasons.
data Poller
  = Poller
  { _pCohorts :: !CohortMap
  , _pIOState :: !(STM.TMVar PollerIOState)
  -- ^ This is in a separate 'STM.TMVar' because it’s important that we are able to construct
  -- 'Poller' values in 'STM.STM' --- we need the insertion into the 'PollerMap' to be atomic to
  -- ensure that we don’t accidentally create two for the same query due to a race. However, we
  -- can’t spawn the worker thread or create the metrics store in 'STM.STM', so we insert it into
  -- the 'Poller' only after we’re certain we won’t create any duplicates.
  }

data PollerIOState
  = PollerIOState
  { _pThread  :: !(A.Async ())
  -- ^ a handle on the poller’s worker thread that can be used to 'A.cancel' it if all its cohorts
  -- stop listening
  , _pMetrics :: !RefetchMetrics
  }

data RefetchMetrics
  = RefetchMetrics
  { _rmSnapshot :: !Metrics.Distribution
  , _rmPush     :: !Metrics.Distribution
  , _rmQuery    :: !Metrics.Distribution
  , _rmTotal    :: !Metrics.Distribution
  }

initRefetchMetrics :: IO RefetchMetrics
initRefetchMetrics = RefetchMetrics <$> Metrics.new <*> Metrics.new <*> Metrics.new <*> Metrics.new

data PollerKey
  -- we don't need operation name here as a subscription will
  -- only have a single top level field
  = PollerKey
  { _lgRole  :: !RoleName
  , _lgQuery :: !MultiplexedQuery
  } deriving (Show, Eq, Generic)

instance Hashable PollerKey

instance J.ToJSON PollerKey where
  toJSON (PollerKey role query) =
    J.object [ "role" J..= role
             , "query" J..= query
             ]

type PollerMap = STMMap.Map PollerKey Poller

dumpPollerMap :: Bool -> PollerMap -> IO J.Value
dumpPollerMap extended lqMap =
  fmap J.toJSON $ do
    entries <- STM.atomically $ ListT.toList $ STMMap.listT lqMap
    forM entries $ \(PollerKey role query, Poller cohortsMap ioState) -> do
      PollerIOState threadId metrics <- STM.atomically $ STM.readTMVar ioState
      metricsJ <- dumpRefetchMetrics metrics
      cohortsJ <-
        if extended
        then Just <$> dumpCohortMap cohortsMap
        else return Nothing
      return $ J.object
        [ "role" J..= role
        , "thread_id" J..= show (A.asyncThreadId threadId)
        , "multiplexed_query" J..= query
        , "cohorts" J..= cohortsJ
        , "metrics" J..= metricsJ
        ]
  where
    dumpRefetchMetrics metrics = do
      snapshotS <- Metrics.read $ _rmSnapshot metrics
      queryS <- Metrics.read $ _rmQuery metrics
      pushS <- Metrics.read $ _rmPush metrics
      totalS <- Metrics.read $ _rmTotal metrics
      return $ J.object
        [ "snapshot" J..= dumpStats snapshotS
        , "query" J..= dumpStats queryS
        , "push" J..= dumpStats pushS
        , "total" J..= dumpStats totalS
        ]

    dumpStats stats =
      J.object
      [ "mean" J..= Metrics.mean stats
      , "variance" J..= Metrics.variance stats
      , "count" J..= Metrics.count stats
      , "min" J..= Metrics.min stats
      , "max" J..= Metrics.max stats
      ]

-- | these functions are for debugging purposes to print out various concurrent data structures
-- showCohortSnapshotMap :: Map.HashMap CohortId CohortSnapshot -> IO ()
-- showCohortSnapshotMap cmap = do
--   let assocList = Map.toList cmap
--   forM_ assocList $ \(cohortId, cohortSnapshot) -> do
--     putStrLn "cohort snaps -->"
--     putStr "cohortId: " >> pPrint cohortId
--     putStrLn "cohortSnapShot: " >> showCohortSnapshot cohortSnapshot

-- showCohortSnapshot :: CohortSnapshot -> IO ()
-- showCohortSnapshot (CohortSnapshot vars prevResp subs newSubs) = do
--   respHash <- STM.atomically $ STM.readTVar prevResp
--   putStrLn "(cohort vars, resp hash, existing subs, new subs)"
--   pPrint (vars, respHash, subs, newSubs)

-- showCohortMapToList :: [(CohortKey, Cohort)] -> IO ()
-- showCohortMapToList xs = do
--   forM_ xs $ \(cohortKey, cohort) -> do
--     putStrLn "cohort map to list"
--     putStr "cohortKey: " >> pPrint cohortKey
--     jVal <- J.encode <$> dumpCohort cohort
--     putStrLn "cohort: " >> pPrint jVal


-- | create an ID to track unique 'Poller's, so that we can gather metrics about each poller
-- TODO(anon): but does it make sense? if there are hundreds of threads running to serve
-- subscriptions to 100s of clients, does it make sense to generate uuids for each thread?
-- shouldn't we just use thread id?
newtype PollerId = PollerId { unPollerId :: UUID.UUID }
  deriving (Show, Eq, Generic, J.ToJSON)

data SimpleCohort
  = SimpleCohort
  { _scCohortId          :: !CohortId
  , _scCohortVariables   :: !CohortVariables
  , _scResponseSizeBytes :: !(Maybe Int)
  , _scSubscribers       :: ![UniqueSubscriberId]
  } deriving (Show, Eq)

$(J.deriveToJSON (J.aesonDrop 3 J.snakeCase) ''SimpleCohort)

data ExecutionBatch
  = ExecutionBatch
  { _ebPgExecutionTime :: !Clock.NominalDiffTime
  -- ^ postgres execution time of each batch
  , _ebPushCohortsTime :: !Clock.NominalDiffTime
  -- ^ time to taken to push to all cohorts belonging to this batch
  , _ebBatchSize       :: !Int
  -- ^ the number of cohorts belonging to this batch
  } deriving (Show, Eq)

$(J.deriveToJSON (J.aesonDrop 3 J.snakeCase) ''ExecutionBatch)

-- | To log out various metrics from the 'Poller' thread in 'pollQuery'
data PollerLog
  = PollerLog
  { _plPollerId           :: !PollerId
  -- ^ the unique ID (basically a thread that run as a 'Poller') for the 'Poller'; TODO: is this
  -- practical? do we need to track, potentially, 1000s of poller threads uniquely?
  , _plSubscribers        :: !(Maybe [(Text, Text)])
  -- ^ list of (WebsocketConnId, OperationId) to uniquely identify each subscriber
  , _plCohortSize         :: !Int
  -- ^ how many cohorts (or no of groups of different variables but same query) are there
  , _plCohorts            :: ![SimpleCohort]
  -- ^ the different query vars which forms the different cohorts
  , _plExecutionBatches   :: ![ExecutionBatch]
  -- ^ list of execution batches and their details
  , _plExecutionBatchSize :: !Int
  -- ^ what is the current batch size that is being run. i.e. in how many batches will this query be
  -- run
  , _plSnapshotTime       :: !Clock.NominalDiffTime
  -- ^ the time taken to get a snapshot of cohorts from our 'LiveQueriesState' data structure
  , _plTotalTime          :: !Clock.NominalDiffTime
  -- ^ total time spent on running the entire 'Poller' thread (which may run more than one thread in
  -- batches for one SQL query) and then push results to each client
  , _plGeneratedSql       :: !Q.Query
  -- ^ the multiplexed SQL query to be run against Postgres with all the variables together
  , _plLiveQueryOptions   :: !LiveQueriesOptions
  } deriving (Show, Eq)

$(J.deriveToJSON (J.aesonDrop 3 J.snakeCase) ''PollerLog)

instance L.ToEngineLog PollerLog L.Hasura where
  toEngineLog pl = (L.LevelInfo, L.ELTInternal L.ILTPollerLog, J.toJSON pl)

-- | Where the magic happens: the top-level action run periodically by each active 'Poller'.
pollQuery
  :: L.Logger L.Hasura
  -> PollerId
  -> RefetchMetrics
  -> LiveQueriesOptions
  -> IsPGExecCtx
  -> MultiplexedQuery
  -> Poller
  -> IO ()
pollQuery logger pollerId metrics lqOpts isPgCtx pgQuery handler = do
  -- start timing, get the process init time
  procInit <- Clock.getCurrentTime

  -- get a snapshot of all the cohorts
  -- this need not be done in a transaction
  cohorts <- STM.atomically $ TMap.toList cohortMap
  cohortSnapshotMap <- Map.fromList <$> mapM (STM.atomically . getCohortSnapshot) cohorts

  -- queryVarsBatches are queryVars broken down into chunks specified by the batch size
  -- TODO(anon): optimize this to fetch all data at one-shot
  let queryVars = getQueryVars cohortSnapshotMap
      queryVarsBatches = chunksOf (unBatchSize batchSize) queryVars
      subscriberWsOpIds = getSubscribers cohortSnapshotMap

  -- get time after a cohorts snapshot is taken
  snapshotFinish <- Clock.getCurrentTime
  let snapshotTime = Clock.diffUTCTime snapshotFinish procInit
  Metrics.add (_rmSnapshot metrics) $ realToFrac snapshotTime

  -- run the multiplexed query, in the partitioned batches
  res <- A.forConcurrently queryVarsBatches $ \queryVarsBatch -> do
    queryInit <- Clock.getCurrentTime

    -- Running multiplexed query for subscription with read-only access
    mxRes <- runExceptT . runLazyROTx' isPgCtx $ executeMultiplexedQuery pgQuery queryVars
    queryFinish <- Clock.getCurrentTime
    let dt = Clock.diffUTCTime queryFinish queryInit
        queryTime = realToFrac dt
        lqMeta = LiveQueryMetadata $ fromUnits dt
        operations = getCohortOperations cohortSnapshotMap lqMeta mxRes
    Metrics.add (_rmQuery metrics) queryTime

    -- concurrently push each unique result
    respSizes <- A.forConcurrently operations $ \(res, respSize, respHash, action, snapshot) ->
      pushResultToCohort res respHash action snapshot >> return respSize

    pushFinish <- Clock.getCurrentTime
    let pushTime = Clock.diffUTCTime pushFinish queryFinish
    Metrics.add (_rmPush metrics) $ realToFrac pushTime

    -- return various metrics of each opeartion of executing against postgres
    return $ (ExecutionBatch dt pushTime (length queryVarsBatch), respSizes)

  procFinish <- Clock.getCurrentTime
  let totalTime = Clock.diffUTCTime procFinish procInit
  Metrics.add (_rmTotal metrics) $ realToFrac totalTime

  -- transform data for PollerLog and log it
  let (execBatches, respSizes') = unzip res
      respSizes = concat respSizes'
      simpleCohorts = zipWith3 (\(cId, vars) (_, size) (_, subs) -> SimpleCohort cId vars size subs)
                      queryVars respSizes subscriberWsOpIds
      pollerLog = PollerLog
                { _plPollerId = pollerId
                , _plSubscribers = Nothing
                , _plCohortSize = length cohorts
                , _plCohorts = simpleCohorts
                , _plExecutionBatches = execBatches
                , _plExecutionBatchSize = length queryVarsBatches
                , _plSnapshotTime = snapshotTime
                , _plTotalTime = totalTime
                , _plGeneratedSql = unMultiplexedQuery pgQuery
                , _plLiveQueryOptions = lqOpts
                }
  L.unLogger logger pollerLog

  where
    Poller cohortMap _ = handler
    LiveQueriesOptions batchSize _ = lqOpts

    -- uncurry4 :: (a -> b -> c -> d -> e) -> (a, b, c, d) -> e
    -- uncurry4 f (a, b, c, d) = f a b c d

    getCohortSnapshot (cohortVars, handlerC) = do
      let Cohort resId respRef curOpsTV newOpsTV = handlerC
      curOpsL <- TMap.toList curOpsTV
      newOpsL <- TMap.toList newOpsTV
      forM_ newOpsL $ \(k, action) -> TMap.insert action k curOpsTV
      TMap.reset newOpsTV
      let cohortSnapshot = CohortSnapshot cohortVars respRef (map snd curOpsL) (map snd newOpsL)
      return (resId, cohortSnapshot)

    getSubscribers :: Map.HashMap CohortId CohortSnapshot -> [(CohortId, [UniqueSubscriberId])]
    getSubscribers cohortSnapshotMap =
      let getWsOpId v = map _sWebsocketOperationId $ _csExistingSubscribers v <> _csNewSubscribers v
      in Map.toList $ Map.map getWsOpId cohortSnapshotMap

    getQueryVars :: Map.HashMap CohortId CohortSnapshot -> [(CohortId, CohortVariables)]
    getQueryVars cohortSnapshotMap =
      Map.toList $ fmap _csVariables cohortSnapshotMap

    getCohortOperations cohortSnapshotMap actionMeta = \case
      Left e ->
        -- TODO: this is internal error
        let resp = GQExecError [encodeGQErr False e]
        in [ (resp, (cId, Nothing), Nothing, actionMeta, snapshot)
           | (cId, snapshot) <- Map.toList cohortSnapshotMap
           ]
      Right responses ->
        flip mapMaybe responses $ \(respId, result) ->
          -- TODO: change it to use bytestrings directly
          let -- No reason to use lazy bytestrings here, since (1) we fetch the entire result set
              -- from Postgres strictly and (2) even if we didn’t, hashing will have to force the
              -- whole thing anyway.
              respBS = encJToBS result
              respHash = mkRespHash respBS
              respSize = BS.length respBS
          in (GQSuccess result, (respId, Just respSize), Just respHash, actionMeta,)
             <$> Map.lookup respId cohortSnapshotMap
