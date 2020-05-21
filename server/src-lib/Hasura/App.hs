{-# LANGUAGE RecordWildCards      #-}
{-# LANGUAGE UndecidableInstances #-}

module Hasura.App where

import           Control.Concurrent.STM.TVar               (readTVarIO)
import           Control.Monad.Base
import           Control.Monad.Stateless
import           Control.Monad.STM                         (atomically)
import           Control.Monad.Trans.Control               (MonadBaseControl (..))
import           Data.Aeson                                ((.=))
import           Data.Time.Clock                           (UTCTime)
import           GHC.AssertNF
import           Options.Applicative
import           System.Environment                        (getEnvironment, lookupEnv)
import           System.Exit                               (exitFailure)

import qualified Control.Concurrent.Async.Lifted.Safe      as LA
import qualified Control.Concurrent.Extended               as C
import qualified Data.Aeson                                as A
import qualified Data.ByteString.Char8                     as BC
import qualified Data.ByteString.Lazy.Char8                as BLC
import qualified Data.Set                                  as Set
import qualified Data.Text                                 as T
import qualified Data.Environment                          as Env
import qualified Data.Time.Clock                           as Clock
import qualified Data.Yaml                                 as Y
import qualified Database.PG.Query                         as Q
import qualified Network.HTTP.Client                       as HTTP
import qualified Network.HTTP.Client.TLS                   as HTTP
import qualified Network.Wai.Handler.Warp                  as Warp
import qualified Text.Mustache.Compile                     as M

import           Hasura.Db
import           Hasura.EncJSON
import           Hasura.Events.Lib
import           Hasura.GraphQL.Execute                    (GQLApiAuthorization (..))
import           Hasura.GraphQL.Logging                    (QueryLog (..))
import           Hasura.GraphQL.Transport.HTTP.Protocol    (toParsed)
import           Hasura.GraphQL.Transport.WebSocket.Server (WSServerLogger (..))

import           Hasura.GraphQL.Resolve.Action             (asyncActionsProcessor)
import           Hasura.Logging
import           Hasura.Prelude
import           Hasura.RQL.Types                          (CacheRWM, Code (..), HasHttpManager,
                                                            HasSQLGenCtx, HasSystemDefined,
                                                            QErr (..), SQLGenCtx (..),
                                                            SchemaCache (..), UserInfoM,
                                                            buildSchemaCacheStrict, decodeValue,
                                                            throw400, withPathK)
import           Hasura.RQL.Types.Run
import           Hasura.Server.API.Query                   (requiresAdmin, runQueryM)
import           Hasura.Server.App
import           Hasura.Server.Auth
import           Hasura.Server.CheckUpdates                (checkForUpdates)
import           Hasura.Server.Init
import           Hasura.Server.Logging
import           Hasura.Server.SchemaUpdate
import           Hasura.Server.Telemetry
import           Hasura.Server.Version
import           Hasura.Session
import qualified Hasura.Tracing                            as Tracing

printErrExit :: (MonadIO m) => forall a . String -> m a
printErrExit = liftIO . (>> exitFailure) . putStrLn

printErrJExit :: (A.ToJSON a, MonadIO m) => forall b . a -> m b
printErrJExit = liftIO . (>> exitFailure) . printJSON

parseHGECommand :: EnabledLogTypes impl => Parser (RawHGECommand impl)
parseHGECommand =
  subparser
    ( command "serve" (info (helper <*> (HCServe <$> serveOptionsParser))
          ( progDesc "Start the GraphQL Engine Server"
            <> footerDoc (Just serveCmdFooter)
          ))
        <> command "export" (info (pure  HCExport)
          ( progDesc "Export graphql-engine's metadata to stdout" ))
        <> command "clean" (info (pure  HCClean)
          ( progDesc "Clean graphql-engine's metadata to start afresh" ))
        <> command "execute" (info (pure  HCExecute)
          ( progDesc "Execute a query" ))
        <> command "downgrade" (info (HCDowngrade <$> downgradeOptionsParser)
          (progDesc "Downgrade the GraphQL Engine schema to the specified version"))
        <> command "version" (info (pure  HCVersion)
          (progDesc "Prints the version of GraphQL Engine"))
    )

parseArgs :: EnabledLogTypes impl => IO (HGEOptions impl)
parseArgs = do
  rawHGEOpts <- execParser opts
  env <- getEnvironment
  let eitherOpts = runWithEnv env $ mkHGEOptions rawHGEOpts
  either printErrExit return eitherOpts
  where
    opts = info (helper <*> hgeOpts)
           ( fullDesc <>
             header "Hasura GraphQL Engine: Realtime GraphQL API over Postgres with access control" <>
             footerDoc (Just mainCmdFooter)
           )
    hgeOpts = HGEOptionsG <$> parseRawConnInfo <*> parseHGECommand

printJSON :: (A.ToJSON a, MonadIO m) => a -> m ()
printJSON = liftIO . BLC.putStrLn . A.encode

printYaml :: (A.ToJSON a, MonadIO m) => a -> m ()
printYaml = liftIO . BC.putStrLn . Y.encode

mkPGLogger :: Logger Hasura -> Q.PGLogger
mkPGLogger (Logger logger) (Q.PLERetryMsg msg) =
  logger $ PGLog LevelWarn msg


-- | most of the required types for initializing graphql-engine
data InitCtx
  = InitCtx
  { _icHttpManager :: !HTTP.Manager
  , _icInstanceId  :: !InstanceId
  -- , _icDbUid       :: !Text
  , _icLoggers     :: !Loggers
  , _icConnInfo    :: !Q.ConnInfo
  , _icPgExecCtx   :: !IsPGExecCtx
  }

-- | Collection of the LoggerCtx, the regular Logger and the PGLogger
-- TODO: better naming?
data Loggers
  = Loggers
  { _lsLoggerCtx :: !(LoggerCtx Hasura)
  , _lsLogger    :: !(Logger Hasura)
  , _lsPgLogger  :: !Q.PGLogger
  }

newtype AppM a = AppM { unAppM :: IO a }
  deriving (Functor, Applicative, Monad, MonadIO, MonadBase IO, MonadBaseControl IO)

-- | this function initializes the catalog and returns an @InitCtx@, based on the command given
-- - for serve command it creates a proper PG connection pool
-- - for other commands, it creates a minimal pool
-- this exists as a separate function because the context (logger, http manager, pg pool) can be
-- used by other functions as well
initialiseCtx
  :: MonadIO m
  => HGECommand Hasura
  -> RawConnInfo
  -> m (InitCtx, UTCTime)
initialiseCtx hgeCmd rci = do
  initTime <- liftIO Clock.getCurrentTime
  -- global http manager
  httpManager <- liftIO $ HTTP.newManager HTTP.tlsManagerSettings
  instanceId <- liftIO generateInstanceId
  connInfo <- liftIO procConnInfo
  (loggers, _, isPGCtx) <- case hgeCmd of
    -- for server command generate a proper pool
    HCServe so@ServeOptions{..} -> do
      l@(Loggers _ logger pgLogger) <- mkLoggers soEnabledLogTypes soLogLevel
      -- log serve options
      unLogger logger $ serveOptsToLog so
      -- log postgres connection info
      unLogger logger $ connInfoToLog connInfo
      pool <- liftIO $ Q.initPGPool connInfo soConnParams pgLogger
      let isPgCtx = mkIsPgCtx pool soTxIso
      -- safe init catalog
      -- initialiseCatalog isPgCtx (SQLGenCtx soStringifyNum) httpManager logger

      return (l, pool, isPgCtx)

    -- for other commands generate a minimal pool
    _ -> do
      l@(Loggers _ _ pgLogger) <- mkLoggers defaultEnabledLogTypes LevelInfo
      pool <- getMinimalPool pgLogger connInfo
      return (l, pool, mkIsPgCtx pool Q.Serializable)

  -- -- get the unique db id, get it in this init step because Pro needs it before the server is run
  -- -- but you can't get the DbUid unless the catalog is initialized (in case of an empty database)
  -- eDbId <- liftIO $ runExceptT $ Q.runTx pool (Q.Serializable, Nothing) getDbId
  -- dbId <- either printErrJExit return eDbId

  return (InitCtx httpManager instanceId loggers connInfo isPGCtx, initTime)
  where
    mkIsPgCtx pool txIso = IsPGExecCtx (const $ pure $ PGExecCtx pool txIso) [pool]
    procConnInfo =
      either (printErrExit . ("Fatal Error : " <>)) return $ mkConnInfo rci

    getMinimalPool pgLogger ci = do
      let connParams = Q.defaultConnParams { Q.cpConns = 1 }
      liftIO $ Q.initPGPool ci connParams pgLogger

    mkLoggers enabledLogs logLevel = do
      loggerCtx <- liftIO $ mkLoggerCtx (defaultLoggerSettings True logLevel) enabledLogs
      let logger = mkLogger loggerCtx
          pgLogger = mkPGLogger logger
      return $ Loggers loggerCtx logger pgLogger

-- | Run a transaction and if an error is encountered, log the error and abort the program
runTxIO :: IsPGExecCtx -> Q.TxAccess -> Q.TxE QErr a -> IO a
runTxIO pool isoLevel tx = do
  eVal <- liftIO $ runExceptT $ runLazyTx isoLevel pool $ liftTx tx
  either printErrJExit return eVal


class Monad m => Telemetry m where
  -- an abstract method to start telemetry
  startTelemetry
    :: HasVersion
    => Logger Hasura -> ServeOptions impl -> SchemaCacheRef -> InitCtx -> m ()

-- TODO: Put Env into ServeOptions?

runHGEServer
  :: ( HasVersion
     , MonadIO m
     , MonadStateless IO m
     , UserAuthentication (Tracing.TraceT m)
     , MetadataApiAuthorization m
     , GQLApiAuthorization m
     , HttpLog m
     , QueryLogger m
     , WSServerLogger m
     , ConsoleRenderer m
     , ConfigApiHandler m
     , Tracing.HasReporter m
     , LA.Forall (LA.Pure m)
     , Telemetry m
     )
  => Env.Environment
  -> ServeOptions impl
  -> InitCtx
  -> UTCTime
  -- ^ start time
  -> C.MVar ()
  -- ^ shutdown latch
  -> m ()
runHGEServer env serveOpts@ServeOptions{..} initCtx@InitCtx{..} initTime shutdownLatch = do
  -- Comment this to enable expensive assertions from "GHC.AssertNF". These will log lines to
  -- STDOUT containing "not in normal form". In the future we could try to integrate this into
  -- our tests. For now this is a development tool.
  --
  -- NOTE: be sure to compile WITHOUT code coverage, for this to work properly.
  liftIO disableAssertNF

  let sqlGenCtx = SQLGenCtx soStringifyNum
      Loggers loggerCtx logger _ = _icLoggers

  authModeRes <- runExceptT $ mkAuthMode soAdminSecret soAuthHook soJwtSecret soUnAuthRole
                              _icHttpManager logger

  authMode <- either (printErrExit . T.unpack) return authModeRes

  HasuraApp app cacheRef cacheInitTime shutdownApp <-
    mkWaiApp env
             logger
             sqlGenCtx
             soEnableAllowlist
             _icPgExecCtx
             _icConnInfo
             _icHttpManager
             authMode
             soCorsConfig
             soEnableConsole
             soConsoleAssetsDir
             soEnableTelemetry
             _icInstanceId
             soEnabledAPIs
             soLiveQueryOpts
             soPlanCacheOptions
             soResponseInternalErrorsConfig

  -- log inconsistent schema objects
  inconsObjs <- scInconsistentObjs <$> liftIO (getSCFromRef cacheRef)
  liftIO $ logInconsObjs logger inconsObjs

  -- start background threads for schema sync
  (_schemaSyncListenerThread, _schemaSyncProcessorThread) <-
    startSchemaSyncThreads sqlGenCtx _icPgExecCtx logger _icHttpManager
                           cacheRef _icInstanceId cacheInitTime


  maxEvThrds <- liftIO $ getFromEnv defaultMaxEventThreads "HASURA_GRAPHQL_EVENTS_HTTP_POOL_SIZE"
  fetchI  <- fmap milliseconds $ liftIO $
    getFromEnv defaultFetchIntervalMilliSec "HASURA_GRAPHQL_EVENTS_FETCH_INTERVAL"
  logEnvHeaders <- liftIO $ getFromEnv False "LOG_HEADERS_FROM_ENV"

  -- prepare event triggers data
  prepareEvents _icPgExecCtx logger
  eventEngineCtx <- liftIO $ atomically $ initEventEngineCtx maxEvThrds fetchI
  unLogger logger $ mkGenericStrLog LevelInfo "event_triggers" "starting workers"

  _eventQueueThread <- C.forkImmortal "processEventQueue" logger $
    processEventQueue logger logEnvHeaders
    _icHttpManager _icPgExecCtx (getSCFromRef cacheRef) eventEngineCtx

  -- start a backgroud thread to handle async actions
  _asyncActionsThread <- C.forkImmortal "asyncActionsProcessor" logger $
    asyncActionsProcessor env (_scrCache cacheRef) _icPgExecCtx _icHttpManager

  -- start a background thread to check for updates
  _updateThread <- C.forkImmortal "checkForUpdates" logger $ liftIO $
    checkForUpdates loggerCtx _icHttpManager

  startTelemetry logger serveOpts cacheRef initCtx

  finishTime <- liftIO Clock.getCurrentTime
  let apiInitTime = realToFrac $ Clock.diffUTCTime finishTime initTime
  unLogger logger $
    mkGenericLog LevelInfo "server" $ StartupTimeInfo "starting API server" apiInitTime
  let warpSettings = Warp.setPort soPort
                     . Warp.setHost soHost
                     . Warp.setGracefulShutdownTimeout (Just 30) -- 30s graceful shutdown
                     . Warp.setInstallShutdownHandler (shutdownHandler logger shutdownApp eventEngineCtx _icPgExecCtx)
                     $ Warp.defaultSettings
  liftIO $ Warp.runSettings warpSettings app

  where
    -- | prepareEvents is a function to unlock all the events that are
    -- locked and unprocessed, which is called while hasura is started.
    -- Locked and unprocessed events can occur in 2 ways
    -- 1.
    -- Hasura's shutdown was not graceful in which all the fetched
    -- events will remain locked and unprocessed(TODO: clean shutdown)
    -- state.
    -- 2.
    -- There is another hasura instance which is processing events and
    -- it will lock events to process them.
    -- So, unlocking all the locked events might re-deliver an event(due to #2).
    prepareEvents isPgCtx (Logger logger) = do
      liftIO $ logger $ mkGenericStrLog LevelInfo "event_triggers" "preparing data"
      res <- runExceptT $ runLazyTx Q.ReadWrite isPgCtx $ liftTx unlockAllEvents
      either printErrJExit return res

    -- | shutdownEvents will be triggered when a graceful shutdown has been inititiated, it will
    -- get the locked events from the event engine context and then it will unlock all those events.
    -- It may happen that an event may be processed more than one time, an event that has been already
    -- processed but not been marked as delivered in the db will be unlocked by `shutdownEvents`
    -- and will be processed when the events are proccessed next time.
    shutdownEvents :: IsPGExecCtx -> Logger Hasura -> EventEngineCtx -> IO ()
    shutdownEvents pool (Logger logger) EventEngineCtx {..} = do
      liftIO $ logger $ mkGenericStrLog LevelInfo "event_triggers" "unlocking events that are locked by the HGE"
      lockedEvents <- readTVarIO _eeCtxLockedEvents
      liftIO $ do
        when (not $ Set.null $ lockedEvents) $ do
          res <- runTx pool Q.ReadWrite (unlockEvents $ toList lockedEvents)
          case res of
            Left err -> logger $ mkGenericStrLog
                         LevelWarn "event_triggers" ("Error in unlocking the events " ++ (show err))
            Right count -> logger $ mkGenericStrLog
                            LevelInfo "event_triggers" ((show count) ++ " events were updated")

    -- This should be isolated to the few env variables required for starting the application server,
    -- Not any variables supplied by the user for URL templating etc.
    getFromEnv :: (Read a) => a -> String -> IO a
    getFromEnv defaults k = do
      mEnv <- lookupEnv k
      let mRes = case mEnv of
            Nothing  -> Just defaults
            Just val -> readMaybe val
          eRes = maybe (Left $ "Wrong expected type for environment variable: " <> k) Right mRes
      either printErrExit return eRes

    runTx :: IsPGExecCtx -> Q.TxAccess -> Q.TxE QErr a -> IO (Either QErr a)
    runTx pool txLevel tx =
      liftIO $ runExceptT $ runLazyTx txLevel pool $ liftTx tx

    -- | Waits for the shutdown latch 'MVar' to be filled, and then
    -- shuts down the server and associated resources.
    -- Structuring things this way lets us decide elsewhere exactly how
    -- we want to control shutdown. 
    shutdownHandler :: Logger Hasura -> IO () -> EventEngineCtx -> IsPGExecCtx -> IO () -> IO ()
    shutdownHandler (Logger logger) shutdownApp eeCtx pool closeSocket =
      LA.link =<< LA.async do
        _ <- C.takeMVar shutdownLatch
        logger $ mkGenericStrLog LevelInfo "server" "gracefully shutting down server"
        shutdownEvents pool (Logger logger) eeCtx
        closeSocket
        shutdownApp

runAsAdmin
  :: (MonadIO m)
  => IsPGExecCtx
  -> SQLGenCtx
  -> HTTP.Manager
  -> Run a
  -> m (Either QErr a)
runAsAdmin isPgCtx sqlGenCtx httpManager m = do
  let runCtx = RunCtx adminUserInfo httpManager sqlGenCtx
  runExceptT $ peelRun runCtx isPgCtx' (runLazyTx Q.ReadWrite) m
  where isPgCtx' = withTxIsolation Q.Serializable isPgCtx

execQuery
  :: ( HasVersion
     , CacheRWM m
     , MonadTx m
     , MonadIO m
     , HasHttpManager m
     , HasSQLGenCtx m
     , UserInfoM m
     , HasSystemDefined m
     )
  => Env.Environment
  -> BLC.ByteString
  -> m BLC.ByteString
execQuery env queryBs = do
  query <- case A.decode queryBs of
    Just jVal -> decodeValue jVal
    Nothing   -> throw400 InvalidJSON "invalid json"
  buildSchemaCacheStrict
  encJToLBS <$> runQueryM env query

instance QueryLogger AppM where
  logQuery logger query genSqlM reqId =
    unLogger logger $ QueryLog query genSqlM reqId

instance WSServerLogger AppM where
  logWSServer = unLogger

instance Tracing.HasReporter AppM

instance HttpLog AppM where
  logHttpError logger userInfoM reqId httpReq req qErr headers =
    unLogger logger $ mkHttpLog $
      mkHttpErrorLogContext userInfoM reqId httpReq qErr req Nothing Nothing headers

  logHttpSuccess logger userInfoM reqId httpReq _ _ compressedResponse qTime cType headers =
    unLogger logger $ mkHttpLog $
      mkHttpAccessLogContext userInfoM reqId httpReq compressedResponse qTime cType headers

instance UserAuthentication (Tracing.TraceT AppM) where
  resolveUserInfo logger manager headers authMode =
    runExceptT $ getUserInfoWithExpTime logger manager headers authMode

instance MetadataApiAuthorization AppM where
  authorizeMetadataApi query userInfo = do
    let currRole = _uiRole userInfo
    when (requiresAdmin query && currRole /= adminRoleName) $
      withPathK "args" $ throw400 AccessDenied errMsg
    where
      errMsg = "restricted access : admin only"

instance GQLApiAuthorization AppM where
  authorizeGQLApi _ _ query = runExceptT $ toParsed query

instance ConfigApiHandler AppM where
  runConfigApiHandler = configApiGetHandler

instance Telemetry AppM where
  startTelemetry logger serveOptions cacheRef InitCtx{..} = do
    -- start a background thread for telemetry
    when (soEnableTelemetry serveOptions) $ do
      unLogger logger $ mkGenericStrLog LevelInfo "telemetry" telemetryNotice

      (dbId, pgVersion) <- liftIO $ runTxIO _icPgExecCtx Q.ReadOnly $
        (,) <$> getDbId <*> getPgVersion

      void $ C.forkImmortal "runTelemetry" logger $ liftIO $
        runTelemetry logger _icHttpManager (getSCFromRef cacheRef) dbId _icInstanceId pgVersion


instance ConsoleRenderer AppM where
  renderConsole path authMode enableTelemetry consoleAssetsDir =
    return $ mkConsoleHTML path authMode enableTelemetry consoleAssetsDir

mkConsoleHTML :: HasVersion => Text -> AuthMode -> Bool -> Maybe Text -> Either String Text
mkConsoleHTML path authMode enableTelemetry consoleAssetsDir =
  renderHtmlTemplate consoleTmplt $
      -- variables required to render the template
      A.object [ "isAdminSecretSet" .= isAdminSecretSet authMode
               , "consolePath" .= consolePath
               , "enableTelemetry" .= boolToText enableTelemetry
               , "cdnAssets" .= boolToText (isNothing consoleAssetsDir)
               , "assetsVersion" .= consoleAssetsVersion
               , "serverVersion" .= currentVersion
               ]
    where
      consolePath = case path of
        "" -> "/console"
        r  -> "/console/" <> r

      consoleTmplt = $(M.embedSingleTemplate "src-rsr/console.html")

telemetryNotice :: String
telemetryNotice =
  "Help us improve Hasura! The graphql-engine server collects anonymized "
  <> "usage stats which allows us to keep improving Hasura at warp speed. "
  <> "To read more or opt-out, visit https://hasura.io/docs/1.0/graphql/manual/guides/telemetry.html"
