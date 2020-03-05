{-# LANGUAGE ViewPatterns #-}
module Hasura.RQL.DDL.RemoteRelationship
  ( runCreateRemoteRelationship
  , runDeleteRemoteRelationship
  , runUpdateRemoteRelationship
  , resolveRemoteRelationship
  , delRemoteRelFromCatalog
  ) where

import           Hasura.EncJSON
import           Hasura.GraphQL.Validate.Types
import           Hasura.Prelude
import           Hasura.RQL.DDL.RemoteRelationship.Validate
import           Hasura.RQL.Types
import           Hasura.SQL.Types

import           Data.Aeson.Types
import           Instances.TH.Lift                          ()

import qualified Database.PG.Query                          as Q

runCreateRemoteRelationship ::
     (MonadTx m, CacheRWM m) => RemoteRelationship -> m EncJSON
runCreateRemoteRelationship remoteRelationship = do
  -- Few checks
  void $ askTabInfo $ rtrTable remoteRelationship
  liftTx $ persistRemoteRelationship remoteRelationship
  buildSchemaCacheFor $ MOTableObj table $ MTORemoteRelationship $ rtrName remoteRelationship
  pure successMsg
  where
    table = rtrTable remoteRelationship

resolveRemoteRelationship
  :: QErrM m
  => RemoteRelationship
  -> [PGColumnInfo]
  -> RemoteSchemaMap
  -> m (RemoteFieldInfo, TypeMap, [SchemaDependency])
resolveRemoteRelationship remoteRelationship pgColumns remoteSchemaMap = do
  (remoteField, typesMap) <- either (throw400 RemoteSchemaError . validateErrorToText)
                             pure
                             (validateRemoteRelationship remoteRelationship remoteSchemaMap pgColumns)

  let schemaDependencies =
        let table = rtrTable remoteRelationship
            columns = _rfiHasuraFields remoteField
            remoteSchemaName = rtrRemoteSchema remoteRelationship
            tableDep = SchemaDependency (SOTable table) DRTable
            columnsDep =
              map
                (\column ->
                   SchemaDependency
                     (SOTableObj table $ TOCol column)
                     DRRemoteRelationship ) $
              map pgiColumn (toList columns)
            remoteSchemaDep =
              SchemaDependency (SORemoteSchema remoteSchemaName) DRRemoteSchema
         in (tableDep : remoteSchemaDep : columnsDep)

  pure (remoteField, typesMap, schemaDependencies)

runUpdateRemoteRelationship :: (MonadTx m, CacheRWM m) => RemoteRelationship -> m EncJSON
runUpdateRemoteRelationship remoteRelationship = do
  fieldInfoMap <- askFieldInfoMap table
  void $ askRemoteRel fieldInfoMap (rtrName remoteRelationship)
  liftTx $ updateRemoteRelInCatalog remoteRelationship
  buildSchemaCacheFor $ MOTableObj table $ MTORemoteRelationship $ rtrName remoteRelationship
  pure successMsg
  where
    table = rtrTable remoteRelationship

persistRemoteRelationship
  :: RemoteRelationship -> Q.TxE QErr ()
persistRemoteRelationship remoteRelationship =
  Q.unitQE defaultTxErrorHandler [Q.sql|
  INSERT INTO hdb_catalog.hdb_remote_relationship
  (name, table_schema, table_name, remote_schema, configuration)
  VALUES ($1, $2, $3, $4, $5 :: jsonb)
  |]
  (let QualifiedObject schema_name table_name = rtrTable remoteRelationship
   in (rtrName remoteRelationship
      ,schema_name
      ,table_name
      ,rtrRemoteSchema remoteRelationship
      ,Q.AltJ remoteRelationship))
  True

updateRemoteRelInCatalog
  :: RemoteRelationship -> Q.TxE QErr ()
updateRemoteRelInCatalog remoteRelationship =
  Q.unitQE defaultTxErrorHandler [Q.sql|
  UPDATE hdb_catalog.hdb_remote_relationship
  SET
  remote_schema = $4,
  configuration = $5
  WHERE name = $1 AND table_schema = $2 AND table_name = $3
  |]
  (let QualifiedObject schema_name table_name = rtrTable remoteRelationship
   in (rtrName remoteRelationship
      ,schema_name
      ,table_name
      ,rtrRemoteSchema remoteRelationship
      ,Q.JSONB (toJSON (remoteRelationship))))
  True

runDeleteRemoteRelationship ::
     (MonadTx m, CacheRWM m) => DeleteRemoteRelationship -> m EncJSON
runDeleteRemoteRelationship (DeleteRemoteRelationship table relName)= do
  fieldInfoMap <- askFieldInfoMap table
  void $ askRemoteRel fieldInfoMap relName
  liftTx $ delRemoteRelFromCatalog table relName
  buildSchemaCacheFor $ MOTableObj table $ MTORemoteRelationship relName
  pure successMsg

delRemoteRelFromCatalog
  :: QualifiedTable
  -> RemoteRelationshipName
  -> Q.TxE QErr ()
delRemoteRelFromCatalog (QualifiedObject sn tn) (RemoteRelationshipName relName) =
  Q.unitQE defaultTxErrorHandler [Q.sql|
           DELETE FROM
                  hdb_catalog.hdb_remote_relationship
           WHERE table_schema =  $1
             AND table_name = $2
             AND name = $3
                |] (sn, tn, relName) True
