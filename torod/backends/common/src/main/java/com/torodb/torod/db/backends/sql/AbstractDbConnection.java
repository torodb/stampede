/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.torodb.torod.db.backends.sql;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.torodb.torod.core.ValueRow;
import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.exceptions.UserToroException;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.pojos.CollectionMetainfo;
import com.torodb.torod.core.pojos.IndexedAttributes;
import com.torodb.torod.core.pojos.NamedToroIndex;
import com.torodb.torod.core.subdocument.SubDocType;
import com.torodb.torod.core.subdocument.SubDocument;
import com.torodb.torod.core.subdocument.structure.DocStructure;
import com.torodb.torod.core.subdocument.values.Value;
import com.torodb.torod.db.backends.DatabaseInterface;
import com.torodb.torod.db.backends.exceptions.InvalidCollectionSchemaException;
import com.torodb.torod.db.backends.meta.IndexStorage;
import com.torodb.torod.db.backends.meta.Routines;
import com.torodb.torod.db.backends.meta.TorodbMeta;
import com.torodb.torod.db.backends.query.QueryEvaluator;
import com.torodb.torod.db.backends.sql.index.IndexManager;
import com.torodb.torod.db.backends.sql.index.NamedDbIndex;
import com.torodb.torod.db.backends.sql.index.UnnamedDbIndex;
import com.torodb.torod.db.backends.sql.utils.SqlWindow;
import com.torodb.torod.db.backends.tables.CollectionsTable;
import com.torodb.torod.db.backends.tables.SubDocTable;
import com.torodb.torod.db.backends.tables.records.CollectionsRecord;
import com.torodb.torod.db.backends.tables.records.SubDocTableRecord;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.*;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonStructure;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;


/**
 *
 */
public abstract class AbstractDbConnection implements
        DbConnection,
        IndexManager.DbIndexCreator,
        IndexManager.DbIndexDropper,
        IndexManager.ToroIndexCreatedListener,
        IndexManager.ToroIndexDroppedListener {
    
    private static final Logger LOGGER
            = LoggerFactory.getLogger(AbstractDbConnection.class);

    private final TorodbMeta meta;
    private final Configuration jooqConf;
    private final DSLContext dsl;
    private final DatabaseInterface databaseInterface;

    @Inject
    public AbstractDbConnection(
            DSLContext dsl,
            TorodbMeta meta, DatabaseInterface databaseInterface) {
        this.jooqConf = dsl.configuration();
        this.meta = meta;
        this.dsl = dsl;
        this.databaseInterface = databaseInterface;
    }

    protected abstract String getCreateSubDocTypeTableQuery(SubDocTable table, Configuration conf);

    protected abstract String getCreateIndexQuery(SubDocTable table, Field<?> field, Configuration conf);
    
    protected abstract void createSchema(String escapedSchemaName) throws SQLException;

    protected abstract void createStructuresTable(String escapedSchemaName) throws SQLException;

    protected abstract void createRootTable(String escapedSchemaName) throws SQLException;

    protected abstract String getRootSeqName();

    protected abstract void createSequence(String escapedSchemaName, String seqName) throws SQLException;
    
    protected DSLContext getDsl() {
        return dsl;
    }

    protected TorodbMeta getMeta() {
        return meta;
    }

    @Override
    public void close() {
        try {
            Connection connection = jooqConf.connectionProvider().acquire();
            connection.close();
        } catch (SQLException ex) {
            //TODO: Change exception
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void rollback() {
        try {
            Connection connection = jooqConf.connectionProvider().acquire();
            connection.rollback();
        } catch (SQLException ex) {
            //TODO: Change exception
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void commit() {
        try {
            Connection connection = jooqConf.connectionProvider().acquire();
            connection.commit();
        } catch (SQLException ex) {
            //TODO: Change exception
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void createCollection(
            @Nonnull String collectionName, 
            @Nullable String schemaName,
            @Nullable JsonObject other) {
        try {
            if (schemaName == null) {
                schemaName = collectionName;
            }
            String escapedSchemaName = databaseInterface.escapeSchemaName(schemaName);
            createSchema(escapedSchemaName);
            createSequence(escapedSchemaName, getRootSeqName());
            createRootTable(escapedSchemaName);
            createStructuresTable(escapedSchemaName);

            String otherAsString = other != null ? other.toString() : null;
            int inserted = dsl.insertInto(CollectionsTable.COLLECTIONS)
                    .set(CollectionsTable.COLLECTIONS.NAME, collectionName)
                    .set(CollectionsTable.COLLECTIONS.SCHEMA, escapedSchemaName)
                    .set(CollectionsTable.COLLECTIONS.CAPPED, false)
                    .set(CollectionsTable.COLLECTIONS.MAX_SIZE, 0)
                    .set(CollectionsTable.COLLECTIONS.MAX_ELEMENTES, 0)
                    .set(CollectionsTable.COLLECTIONS.OTHER, otherAsString)
                    .set(CollectionsTable.COLLECTIONS.STORAGE_ENGINE, "torodb-dev")
                    .execute();
            assert inserted == 1;
            
            meta.createCollectionSchema(collectionName, schemaName, dsl);
        } catch (DataAccessException ex) {
            //TODO: Change exception
            throw new RuntimeException(ex);
        }
        catch (SQLException ex) {
            //TODO: Change exception
            throw new RuntimeException(ex);
        }
        catch (InvalidCollectionSchemaException ex) {
            //TODO: Change exception
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void createSubDocTypeTable(String collection, SubDocType type) throws ImplementationDbException {
        try {
            IndexStorage.CollectionSchema collectionSchema = meta.getCollectionSchema(collection);
            if (!collectionSchema.existsSubDocTable(type)) {
                SubDocTable subDocTable = meta.getCollectionSchema(collection).prepareSubDocTable(type);

                getDsl().execute(getCreateSubDocTypeTableQuery(subDocTable, jooqConf));

                getDsl().execute(getCreateIndexQuery(subDocTable, subDocTable.getDidColumn(), jooqConf));
            }
        } catch (DataAccessException ex) {
            //TODO: Change exception
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void reserveDocIds(String collection, int idsToReserve) throws ImplementationDbException {
        if (idsToReserve == 0) {
            //TODO: log
            return;
        }

        IndexStorage.CollectionSchema colSchema = meta.getCollectionSchema(collection);
        try {
            Routines.reserveDocIds(
                    jooqConf,
                    colSchema,
                    idsToReserve);
        } catch (DataAccessException ex) {
            //TODO: Change exception
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void insertSubdocuments(String collection, SubDocType type, Iterator<? extends SubDocument> subDocuments) {
        try {
            Collection<Query> inserts = Lists.newLinkedList();

            SubDocTable table = meta.getCollectionSchema(collection).getSubDocTable(type);

            while (subDocuments.hasNext()) {
                SubDocument subDocument = subDocuments.next();
                assert subDocument.getType().equals(type);

                SubDocTableRecord record = new SubDocTableRecord(table);
                record.setDid(subDocument.getDocumentId());
                record.setIndex(translateSubDocIndexToDatabase(subDocument.getIndex()));
                record.setSubDoc(subDocument);
                InsertSetMoreStep<?> insert = dsl.insertInto(table)
                        .set(record);

                inserts.add(insert);
//                insert.execute();
            }

            dsl.batch(inserts).execute();
        } catch (DataAccessException ex) {
            //TODO: Change exception
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Collection<? extends NamedToroIndex> getIndexes(String collection) {
        try {
            IndexStorage.CollectionSchema colSchema = meta.getCollectionSchema(collection);
            return colSchema.getIndexManager().getIndexes();
        } catch (IllegalArgumentException ex) {
            throw new UserToroException("Collection '" + collection + "' does no exist", ex);
        }
    }

    @Override
    public NamedToroIndex createIndex(
            String collection,
            String indexName, 
            IndexedAttributes attributes, 
            boolean unique, 
            boolean blocking) {
        IndexStorage.CollectionSchema colSchema = meta.getCollectionSchema(collection);
        return colSchema.getIndexManager().createIndex(
                indexName, 
                attributes, 
                unique, 
                blocking,
                this,
                this
        );
    }
    
    @Override
    public boolean dropIndex(String collection, String indexName) {
        return meta.getCollectionSchema(collection).getIndexManager().dropIndex(
                indexName, 
                this,
                this
        );
    }

    private Integer translateSubDocIndexToDatabase(int index) {
        if (index == 0) {
            return null;
        }
        return index;
    }

    @Override
    public Map<String, Integer> findCollections() {
        Collection<IndexStorage.CollectionSchema> collections = meta.getCollectionSchemes();

        Map<String, Integer> result = Maps.newHashMapWithExpectedSize(collections.size());

        for (IndexStorage.CollectionSchema colSchema : collections) {
            int firstFree = Routines.firstFreeDocId(jooqConf, colSchema);
            result.put(colSchema.getCollection(), firstFree);

            Integer lastUsed = (Integer) getDsl().select(DSL.max(DSL.field("did")))
                    .from(DSL.table(DSL.name(colSchema.getName(), "root")))
                    .fetchOne().value1();

            LOGGER.debug("Collection {} has {} as last used id", colSchema.getName(), lastUsed);
            
            if (lastUsed == null) {
                lastUsed = 0;
            }

            if (lastUsed >= firstFree) {
                LOGGER.warn(
                        "Collection {}.root: last used = {}. First free = {}", 
                        new Object[]{
                            colSchema.getName(),
                            lastUsed,
                            firstFree
                        }
                );
            }
        }

        return result;
    }

    @Override
    public int delete(String collection, @Nullable QueryCriteria condition, boolean justOne) {
        IndexStorage.CollectionSchema colSchema = meta.getCollectionSchema(collection);
        QueryEvaluator queryEvaluator = new QueryEvaluator(colSchema, databaseInterface);

        Multimap<DocStructure, Integer> didsByStructure = queryEvaluator.evaluateDidsByStructure(condition, dsl);

        return Routines.deleteDocuments(jooqConf, colSchema, didsByStructure, justOne, databaseInterface);
    }

    @Override
    public void dropCollection(String collection) {
        IndexStorage.CollectionSchema colSchema = meta.getCollectionSchema(collection);
        Routines.dropCollection(jooqConf, colSchema, databaseInterface);
        meta.dropCollectionSchema(collection);
    }

    @Override
    public NamedDbIndex createIndex(IndexStorage.CollectionSchema colSchema, UnnamedDbIndex unnamedDbIndex) {
        return colSchema
                .getIndexStorage()
                .createIndex(dsl, unnamedDbIndex);
    }

    @Override
    public void dropIndex(IndexStorage.CollectionSchema colSchema, NamedDbIndex index) {
        colSchema
                .getIndexStorage()
                .dropIndex(dsl, index);
    }

    @Override
    public List<CollectionMetainfo> getCollectionsMetainfo() {
        
        Result<CollectionsRecord> records = dsl
                .selectFrom(CollectionsTable.COLLECTIONS)
                .fetch();
        
        List<CollectionMetainfo> result = Lists.newArrayListWithCapacity(records.size());
        for (CollectionsRecord record : records) {
            String otherAsString = record.getOther();
            JsonStructure other;
            if (otherAsString == null) {
                other = null;
            }
            else {
                JsonReader reader = Json.createReader(new StringReader(otherAsString));
                other = reader.read();
            }
            result.add(
                    new CollectionMetainfo(
                            record.getName(), 
                            record.isCapped(), 
                            record.getMaxSize(), 
                            record.getMaxElementes(), 
                            other,
                            record.getStorageEngine()
                    )
            );
        }
        
        return result;
    }

    @Override
    public void eventToroIndexCreated(NamedToroIndex index) {
        meta.getCollectionSchema(index.getCollection())
                .getIndexStorage()
                .eventToroIndexCreated(dsl, index);
    }

    @Override
    public void eventToroIndexRemoved(IndexStorage.CollectionSchema colSchema, String indexName) {
        colSchema.getIndexStorage().eventToroIndexRemoved(dsl, indexName);
    }
    
    @Override
    public Integer count(String collection, QueryCriteria query) {
        IndexStorage.CollectionSchema colSchema = meta.getCollectionSchema(collection);

        QueryEvaluator queryEvaluator = new QueryEvaluator(colSchema, databaseInterface);

        Set<Integer> dids = queryEvaluator.evaluateDid(
                query,
                dsl,
                0
        );
        
        return dids.size();
    }

    @SuppressFBWarnings(
            value = "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE",
            justification = "It is known that this command is unsafe. We need"
                    + "to improve it as soon as we can")
    @Override
    public Iterator<ValueRow<Value>> select(String query) throws UserToroException {
        Connection connection = jooqConf.connectionProvider().acquire();
        try {
            try (Statement st = connection.createStatement()) {
                //This is executed to force read only executions
                st.executeUpdate("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
                st.executeUpdate("SET TRANSACTION READ ONLY");
                st.executeUpdate("SET TRANSACTION DEFERRABLE");
                //Once the first query is executed, transacion level is immutable
                ResultSet fakeRS = st.executeQuery("SELECT 1");
                fakeRS.close();


                try (ResultSet rs = st.executeQuery(query)) {
                    return new SqlWindow(rs, databaseInterface.getBasicTypeToSqlType());
                }
            } catch (SQLException ex) {
                //TODO: Change exception
                throw new UserToroException(ex);
            }
        } finally {
            jooqConf.connectionProvider().release(connection);
        }
    }

}
