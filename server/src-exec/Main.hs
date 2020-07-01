{-# LANGUAGE RecordWildCards #-}

module Main where

import           Control.Exception
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
import qualified Data.ByteString.Char8      as BC
import qualified Data.ByteString.Lazy       as BL
import qualified Data.ByteString.Lazy.Char8 as BLC
import qualified Data.Environment           as Env
import qualified Database.PG.Query          as Q
import qualified System.Exit                as Sys
import qualified System.Posix.Signals       as Signals

main :: IO ()
main = do
  tryExit $ do
    args <- parseArgs
    env  <- Env.getEnvironment
    unAppM (runApp env args)
  where
    tryExit io = try io >>= \case
      Left (ExitException code msg) -> BC.putStrLn msg >> Sys.exitWith (Sys.ExitFailure code)
      Right r -> return r

runApp :: Env.Environment -> HGEOptions Hasura -> AppM ()
runApp env (HGEOptionsG rci hgeCmd) =
  withVersion $$(getVersionFromEnvironment) $ case hgeCmd of
    HCServe serveOptions -> do
      (initCtx, initTime) <- initialiseCtx env hgeCmd rci
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
      runHGEServer env serveOptions initCtx Nothing initTime (shutdownLatch, shutdownApp)

    HCExport -> do
      (initCtx, _) <- initialiseCtx env hgeCmd rci
      res <- runTx' initCtx fetchMetadata Q.ReadCommitted
      either (printErrJExit 9) printJSON res

    HCClean -> do
      (initCtx, _) <- initialiseCtx env hgeCmd rci
      res <- runTx' initCtx dropCatalog Q.ReadCommitted
      either (printErrJExit 10) (const cleanSuccess) res

    HCExecute -> do
      (InitCtx{..}, _) <- initialiseCtx env hgeCmd rci
      queryBs <- liftIO BL.getContents
      let sqlGenCtx = SQLGenCtx False
      res <- runAsAdmin _icPgPool sqlGenCtx _icHttpManager $ do
        schemaCache <- buildRebuildableSchemaCache env
        execQuery env queryBs
          & runHasSystemDefinedT (SystemDefined False)
          & runCacheRWT schemaCache
          & fmap (\(res, _, _) -> res)
      either (printErrJExit 11) (liftIO . BLC.putStrLn) res

    HCDowngrade opts -> do
      (InitCtx{..}, initTime) <- initialiseCtx env hgeCmd rci
      let sqlGenCtx = SQLGenCtx False
      res <- downgradeCatalog opts initTime
             & runAsAdmin _icPgPool sqlGenCtx _icHttpManager
      either (printErrJExit 12) (liftIO . print) res

    HCVersion -> liftIO $ putStrLn $ "Hasura GraphQL Engine: " ++ convertText currentVersion
  where
    runTx' initCtx tx txIso =
      liftIO $ runExceptT $ Q.runTx (_icPgPool initCtx) (txIso, Nothing) tx

    cleanSuccess = liftIO $ putStrLn "successfully cleaned graphql-engine related data"
