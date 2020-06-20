{-# LANGUAGE RecordWildCards #-}

module Main where

import           Data.Text.Conversions      (convertText)

import           Hasura.App
import           Hasura.Logging             (Hasura)
import           Hasura.Prelude
import           Hasura.RQL.DDL.Metadata    (fetchMetadata)
import           Hasura.RQL.DDL.Schema
import           Hasura.RQL.Types
import           Hasura.Server.Init
import           Hasura.Server.Migrate      (downgradeCatalog, dropCatalog)
import           Hasura.Server.Version

import qualified Control.Concurrent.MVar    as Conc
import qualified Data.ByteString.Lazy       as BL
import qualified Data.ByteString.Lazy.Char8 as BLC
import qualified Data.Environment           as Env
import qualified Database.PG.Query          as Q
import qualified System.Posix.Signals       as Signals

main :: IO ()
main = do
  args <- parseArgs
  env  <- Env.getEnvironment
  unAppM (runApp env args)

runApp :: Env.Environment -> HGEOptions Hasura -> AppM ()
runApp env (HGEOptionsG rci hgeCmd) =
  withVersion $$(getVersionFromEnvironment) $ case hgeCmd of
    HCServe serveOptions -> do
      (initCtx, initTime) <- initialiseCtx hgeCmd rci
      shutdownLatch <- liftIO Conc.newEmptyMVar
      let shutdownApp = return ()
      -- Catches the SIGTERM signal and initiates a graceful shutdown. Graceful shutdown for regular HTTP
      -- requests is already implemented in Warp, and is triggered by invoking the 'closeSocket' callback.
      -- We only catch the SIGTERM signal once, that is, if the user hits CTRL-C once again, we terminate
      -- the process immediately.
      _ <- liftIO $ Signals.installHandler
        Signals.sigTERM
        (Signals.CatchOnce (Conc.putMVar shutdownLatch ()))
        Nothing
      runHGEServer env serveOptions initCtx initTime (shutdownLatch, shutdownApp)

    HCExport -> do
      (initCtx, _) <- initialiseCtx hgeCmd rci
      res <- runTx' initCtx fetchMetadata
      either (printErrJExit 9) printJSON res

    HCClean -> do
      (initCtx, _) <- initialiseCtx hgeCmd rci
      res <- runTx' initCtx dropCatalog
      either (printErrJExit 10) (const cleanSuccess) res

    HCExecute -> do
      (InitCtx{..}, _) <- initialiseCtx hgeCmd rci
      queryBs <- liftIO BL.getContents
      let sqlGenCtx = SQLGenCtx False
      res <- runAsAdmin _icPgExecCtx sqlGenCtx _icHttpManager $ do
        schemaCache <- buildRebuildableSchemaCache env
        execQuery env queryBs
          & runHasSystemDefinedT (SystemDefined False)
          & runCacheRWT schemaCache
          & fmap (\(res, _, _) -> res)
      either (printErrJExit 11) (liftIO . BLC.putStrLn) res

    HCDowngrade opts -> do
      (InitCtx{..}, initTime) <- initialiseCtx hgeCmd rci
      let sqlGenCtx = SQLGenCtx False
      res <- downgradeCatalog opts initTime
             & runAsAdmin _icPgExecCtx sqlGenCtx _icHttpManager
      either (printErrJExit 12) (liftIO . print) res

    HCVersion -> liftIO $ putStrLn $ "Hasura GraphQL Engine: " ++ convertText currentVersion
  where
    runTx' InitCtx{..} tx =
      liftIO $ runExceptT $ runLazyTx Q.ReadWrite (withTxIsolation Q.Serializable _icPgExecCtx) $ liftTx tx

    cleanSuccess = liftIO $ putStrLn "successfully cleaned graphql-engine related data"
