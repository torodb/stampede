
package com.torodb.backend.meta;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Meta;
import org.jooq.Result;
import org.jooq.Table;

import com.torodb.backend.ErrorHandler.Context;
import com.torodb.backend.SqlHelper;
import com.torodb.backend.SqlInterface;
import com.torodb.backend.exceptions.InvalidDatabaseException;
import com.torodb.backend.exceptions.InvalidDatabaseSchemaException;
import com.torodb.backend.tables.MetaCollectionTable;
import com.torodb.backend.tables.MetaDatabaseTable;
import com.torodb.backend.tables.MetaDocPartTable;
import com.torodb.backend.tables.MetaFieldTable;
import com.torodb.backend.tables.MetaScalarTable;
import com.torodb.backend.tables.records.MetaCollectionRecord;
import com.torodb.backend.tables.records.MetaDatabaseRecord;
import com.torodb.backend.tables.records.MetaDocPartRecord;
import com.torodb.backend.tables.records.MetaFieldRecord;
import com.torodb.backend.tables.records.MetaScalarRecord;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.transaction.metainf.MetainfoRepository;
import com.torodb.core.transaction.metainf.MetainfoRepository.SnapshotStage;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;

/**
 *
 */
public class SnapshotUpdater {

    private static final Logger LOGGER = LogManager.getLogger(SnapshotUpdater.class);

    private SnapshotUpdater() {}

    /**
     * Updates the given metainf repository to add all meta structures stored on the database.
     *
     * @param metainfoRepository The repository where meta structures will be added. It should be empty.
     * @param sqlInterface
     * @param tableRefFactory
     * @throws InvalidDatabaseException
     */
    public static void updateSnapshot(
            MetainfoRepository metainfoRepository,
            SqlInterface sqlInterface,
            SqlHelper sqlHelper,
            SchemaUpdater schemaUpdater,
            TableRefFactory tableRefFactory)
    throws InvalidDatabaseException {
        MutableMetaSnapshot mutableSnapshot;
        try (SnapshotStage stage = metainfoRepository.startSnapshotStage()) {
            mutableSnapshot = stage.createMutableSnapshot();
        }

        if (mutableSnapshot.streamMetaDatabases().anyMatch((o) -> true)) {
            LOGGER.warn("Trying to update a not empty metainfo repository with information from "
                    + "the database");
        }

        try (Connection connection = sqlInterface.getDbBackend().createSystemConnection()) {
            DSLContext dsl = sqlInterface.getDslContextFactory().createDSLContext(connection);
            Meta jooqMeta = dsl.meta();

            schemaUpdater.checkOrCreate(dsl, jooqMeta, sqlInterface, sqlHelper);

            Updater updater = new Updater(dsl, jooqMeta, tableRefFactory, sqlInterface);
            updater.loadMetaSnapshot(mutableSnapshot);

            connection.commit();
        } catch(IOException ioException) {

            throw new InvalidDatabaseException(ioException);
        } catch(SQLException sqlException) {
            sqlInterface.getErrorHandler().handleRollbackException(Context.UNKNOWN, sqlException);

            throw new InvalidDatabaseException(sqlException);
        }


        metainfoRepository.startMerge(mutableSnapshot)
                .close();
    }

    private static class Updater {

        private final DSLContext dsl;
        private final Meta jooqMeta;
        private final TableRefFactory tableRefFactory;
        private final SqlInterface sqlInterface;
        private final MetaCollectionTable<MetaCollectionRecord> collectionTable;
        private final MetaDocPartTable<Object, MetaDocPartRecord<Object>> docPartTable;
        private final MetaFieldTable<Object, MetaFieldRecord<Object>> fieldTable;
        private final MetaScalarTable<Object, MetaScalarRecord<Object>> scalarTable;

        public Updater(DSLContext dsl, Meta jooqMeta, TableRefFactory tableRefFactory, SqlInterface sqlInterface) {
            this.dsl = dsl;
            this.jooqMeta = jooqMeta;
            this.tableRefFactory = tableRefFactory;
            this.sqlInterface = sqlInterface;

            this.collectionTable = sqlInterface.getMetaDataReadInterface().getMetaCollectionTable();
            this.docPartTable = sqlInterface.getMetaDataReadInterface().getMetaDocPartTable();
            this.fieldTable = sqlInterface.getMetaDataReadInterface().getMetaFieldTable();
            this.scalarTable = sqlInterface.getMetaDataReadInterface().getMetaScalarTable();
        }

        private void loadMetaSnapshot(MutableMetaSnapshot mutableSnapshot) throws InvalidDatabaseSchemaException {

            MetaDatabaseTable<MetaDatabaseRecord> metaDatabaseTable = sqlInterface.getMetaDataReadInterface().getMetaDatabaseTable();
            Result<MetaDatabaseRecord> records
                    = dsl.selectFrom(metaDatabaseTable)
                        .fetch();

            for (MetaDatabaseRecord databaseRecord : records) {
                analyzeDatabase(mutableSnapshot, databaseRecord);
            }
        }

        private void analyzeDatabase(MutableMetaSnapshot mutableSnapshot, MetaDatabaseRecord databaseRecord) {
            String dbName = databaseRecord.getName();
            String schemaName = databaseRecord.getIdentifier();

            MutableMetaDatabase metaDatabase = mutableSnapshot.addMetaDatabase(dbName, schemaName);

            SchemaValidator schemaValidator = new SchemaValidator(jooqMeta, schemaName, dbName);

            dsl.selectFrom(collectionTable)
                    .where(collectionTable.DATABASE.eq(dbName))
                    .fetch()
                    .forEach(
                            (col) -> analyzeCollection(metaDatabase, col, schemaValidator)
                    );

            checkCompleteness(dbName, schemaValidator, schemaName);
        }

        private void checkCompleteness(String dbName, SchemaValidator schemaValidator, String schemaName) {
            Map<String, MetaDocPartRecord<Object>> docParts = dsl
                    .selectFrom(docPartTable)
                    .where(docPartTable.DATABASE.eq(dbName))
                    .fetchMap(docPartTable.IDENTIFIER);
            List<MetaFieldRecord<Object>> fields = dsl
                    .selectFrom(fieldTable)
                    .where(fieldTable.DATABASE.eq(dbName))
                    .fetch();
            List<MetaScalarRecord<Object>> scalars = dsl
                    .selectFrom(scalarTable)
                    .where(scalarTable.DATABASE.eq(dbName))
                    .fetch();
            for (Table<?> table : schemaValidator.getExistingTables()) {
                MetaDocPartRecord<?> docPart = docParts.get(table.getName());
                if (docPart == null) {
                    throw new InvalidDatabaseSchemaException(schemaName, "Table "+schemaName+"."+table.getName()
                    +" has no container associated for database "+dbName);
                }

                for (Field<?> existingField : table.fields()) {
                    if (!sqlInterface.getIdentifierConstraints().isAllowedColumnIdentifier(existingField.getName())) {
                        continue;
                    }
                    if (!SchemaValidator.containsField(existingField, docPart.getCollection(),
                            docPart.getTableRefValue(tableRefFactory), fields, scalars, tableRefFactory)) {
                        throw new InvalidDatabaseSchemaException(schemaName, "Column "+schemaName+"."+table.getName()
                        +"."+existingField.getName()+" has no field associated for database "+dbName);
                    }
                }
            }
        }

        private void analyzeCollection(MutableMetaDatabase db, MetaCollectionRecord collection, SchemaValidator schemaValidator) {

            String database = db.getName();
            String collectionName = collection.getName();

            MutableMetaCollection col = db.addMetaCollection(
                    collectionName,
                    collection.getIdentifier()
            );

            dsl.selectFrom(docPartTable)
                    .where(docPartTable.DATABASE.eq(database)
                        .and(docPartTable.COLLECTION.eq(collectionName)))
                    .fetch()
                    .forEach(
                            (docPart) -> analyzeDocPart(db, col, docPart, schemaValidator)
                    );
        }

        private void analyzeDocPart(MutableMetaDatabase db,
                MutableMetaCollection metaCollection, MetaDocPartRecord<Object> docPart,
                SchemaValidator schemaValidator) {
            if (!docPart.getCollection().equals(metaCollection.getName())) {
                return;
            }
            String docPartIdentifier = docPart.getIdentifier();

            TableRef tableRef = docPart.getTableRefValue(tableRefFactory);
            MutableMetaDocPart metaDocPart = metaCollection.addMetaDocPart(tableRef, docPartIdentifier);

            if (!schemaValidator.existsTable(docPartIdentifier)) {
                throw new InvalidDatabaseSchemaException(db.getIdentifier(),
                        "Doc part " + tableRef + " in database " + db.getName()
                        + " is associated with table " + docPartIdentifier
                        + " but there is no table with that name in schema "
                        + db.getIdentifier());
            }
            dsl.selectFrom(fieldTable)
                    .where(fieldTable.DATABASE.eq(db.getName())
                            .and(fieldTable.COLLECTION.eq(metaCollection.getName()))
                            .and(fieldTable.TABLE_REF.eq(docPart.getTableRef())))
                    .fetch()
                    .forEach(
                            (field) -> analyzeField(db, metaDocPart, field, schemaValidator)
                    );

            dsl.selectFrom(scalarTable)
                    .where(scalarTable.DATABASE.eq(db.getName())
                            .and(scalarTable.COLLECTION.eq(metaCollection.getName()))
                            .and(scalarTable.TABLE_REF.eq(docPart.getTableRef())))
                    .fetch()
                    .forEach(
                            (scalar) -> analyzeScalar(db, metaDocPart, scalar, schemaValidator)
                    );
        }

        private void analyzeField(MutableMetaDatabase db, MutableMetaDocPart metaDocPart,
                MetaFieldRecord<?> field, SchemaValidator schemaValidator) {

            String docPartIdentifier = metaDocPart.getIdentifier();
            String schemaName = db.getIdentifier();
            String dbName = db.getName();

            TableRef fieldTableRef = field.getTableRefValue(tableRefFactory);
            if (!metaDocPart.getTableRef().equals(fieldTableRef)) {
                return;
            }

            metaDocPart.addMetaField(field.getName(), field.getIdentifier(), field.getType());

            if (!schemaValidator.existsColumn(docPartIdentifier, field.getIdentifier())) {
                throw new InvalidDatabaseSchemaException(schemaName, "Field "
                        + field.getCollection() + "." + field.getTableRefValue(tableRefFactory)
                        + "." + field.getName() + " of type " + field.getType() + " in database "
                        + dbName + " is associated with field " + field.getIdentifier()
                        + " but there is no field with that name in table " + schemaName + "."
                        + docPartIdentifier);
            }
            if (!schemaValidator.existsColumnWithType(docPartIdentifier, field.getIdentifier(),
                    sqlInterface.getDataTypeProvider().getDataType(field.getType()))) {
                //TODO: some types can not be recognized using meta data
                //throw new InvalidDatabaseSchemaException(schemaName, "Field "+field.getCollection()+"."
                //        +field.getTableRefValue()+"."+field.getName()+" in database "+database+" is associated with field "+field.getIdentifier()
                //        +" and type "+sqlInterface.getDataType(field.getType()).getTypeName()
                //        +" but the field "+schemaName+"."+docPartIdentifier+"."+field.getIdentifier()
                //        +" has a different type "+getColumnType(docPartIdentifier, field.getIdentifier(), existingTables).getTypeName());
            }
        }

        private void analyzeScalar(MutableMetaDatabase db,MutableMetaDocPart metaDocPart,
                MetaScalarRecord<?> scalar, SchemaValidator schemaValidator) {

            String docPartIdentifier = metaDocPart.getIdentifier();
            String schemaName = db.getIdentifier();
            String dbName = db.getName();

            TableRef fieldTableRef = scalar.getTableRefValue(tableRefFactory);
            if (!metaDocPart.getTableRef().equals(fieldTableRef)) {
                return ;
            }

            metaDocPart.addMetaScalar(scalar.getIdentifier(), scalar.getType());

            if (!schemaValidator.existsColumn(docPartIdentifier, scalar.getIdentifier())) {
                throw new InvalidDatabaseSchemaException(schemaName, "Scalar "+scalar.getCollection()+"."
                        +scalar.getTableRefValue(tableRefFactory)+" of type "+scalar.getType()
                        +" in database "+dbName+" is associated with scalar "+scalar.getIdentifier()
                        +" but there is no scalar with that name in table "
                        +schemaName+"."+docPartIdentifier);
            }
            if (!schemaValidator.existsColumnWithType(docPartIdentifier, scalar.getIdentifier(),
                    sqlInterface.getDataTypeProvider().getDataType(scalar.getType()))) {
                //TODO: some types can not be recognized using meta data
                //throw new InvalidDatabaseSchemaException(schemaName, "Scalar "+scalar.getCollection()+"."
                //        +scalar.getTableRefValue()+"."+scalar.getName()+" in database "+database+" is associated with scalar "+scalar.getIdentifier()
                //        +" and type "+sqlInterface.getDataType(scalar.getType()).getTypeName()
                //        +" but the scalar "+schemaName+"."+docPartIdentifier+"."+scalar.getIdentifier()
                //        +" has a different type "+getColumnType(docPartIdentifier, scalar.getIdentifier(), existingTables).getTypeName());
            }
        }
    }

}
