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

import java.io.StringReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonStructure;

import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.InsertSetMoreStep;
import org.jooq.Result;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.torodb.torod.core.connection.exceptions.RetryTransactionException;
import com.torodb.torod.core.d2r.D2RTranslator;
import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.exceptions.UserToroException;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.pojos.CollectionMetainfo;
import com.torodb.torod.core.pojos.IndexedAttributes;
import com.torodb.torod.core.pojos.NamedToroIndex;
import com.torodb.torod.core.subdocument.SplitDocument;
import com.torodb.torod.core.subdocument.SubDocType;
import com.torodb.torod.core.subdocument.SubDocument;
import com.torodb.torod.core.subdocument.ToroDocument;
import com.torodb.torod.core.subdocument.structure.DocStructure;
import com.torodb.torod.db.backends.DatabaseInterface;
import com.torodb.torod.db.backends.exceptions.InvalidCollectionSchemaException;
import com.torodb.torod.db.backends.meta.CollectionSchema;
import com.torodb.torod.db.backends.meta.Routines;
import com.torodb.torod.db.backends.meta.TorodbMeta;
import com.torodb.torod.db.backends.meta.routines.QueryRoutine;
import com.torodb.torod.db.backends.query.QueryEvaluator;
import com.torodb.torod.db.backends.sql.index.IndexManager;
import com.torodb.torod.db.backends.sql.index.NamedDbIndex;
import com.torodb.torod.db.backends.sql.index.UnnamedDbIndex;
import com.torodb.torod.db.backends.tables.SubDocTable;
import com.torodb.torod.db.backends.tables.records.AbstractCollectionsRecord;
import com.torodb.torod.db.backends.tables.records.SubDocTableRecord;


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
    private final D2RTranslator d2r;
    private final QueryRoutine queryRoutine;
    private final DatabaseInterface databaseInterface;
    private final Provider<SubDocType.Builder> subDocTypeBuilderProvider;

    @Inject
    public AbstractDbConnection(
            DSLContext dsl,
            TorodbMeta meta,
            Provider<SubDocType.Builder> subDocTypeBuilderProvider,
            D2RTranslator d2r,
            QueryRoutine queryRoutine,
            DatabaseInterface databaseInterface) {
        this.jooqConf = dsl.configuration();
        this.meta = meta;
        this.dsl = dsl;
        this.subDocTypeBuilderProvider = subDocTypeBuilderProvider;
        this.d2r = d2r;
        this.queryRoutine = queryRoutine;
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

    protected Configuration getJooqConf() {
        return jooqConf;
    }

    protected DatabaseInterface getDatabaseInterface() {
        return databaseInterface;
    }

    @Override
    public void close() {
        try {
            Connection connection = jooqConf.connectionProvider().acquire();
            if (!connection.isClosed()) {
                connection.rollback();
                connection.close();
            }
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
    public Savepoint setSavepoint() {
        try {
            Connection connection = jooqConf.connectionProvider().acquire();
            return connection.setSavepoint();
        } catch (SQLException ex) {
            //TODO: Change exception
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void rollback(Savepoint savepoint) {
        try {
            Connection connection = jooqConf.connectionProvider().acquire();
            connection.rollback(savepoint);
        } catch (SQLException ex) {
            //TODO: Change exception
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) {
        try {
            Connection connection = jooqConf.connectionProvider().acquire();
            connection.releaseSavepoint(savepoint);
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
            int inserted = dsl.insertInto(databaseInterface.getCollectionsTable())
                    .set(databaseInterface.getCollectionsTable().NAME, collectionName)
                    .set(databaseInterface.getCollectionsTable().SCHEMA, escapedSchemaName)
                    .set(databaseInterface.getCollectionsTable().CAPPED, false)
                    .set(databaseInterface.getCollectionsTable().MAX_SIZE, 0)
                    .set(databaseInterface.getCollectionsTable().MAX_ELEMENTES, 0)
                    .set(databaseInterface.getCollectionsTable().OTHER, otherAsString)
                    .set(databaseInterface.getCollectionsTable().STORAGE_ENGINE, "torodb-dev")
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
            CollectionSchema collectionSchema = meta.getCollectionSchema(collection);
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

        CollectionSchema colSchema = meta.getCollectionSchema(collection);
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
    public void insertSubdocuments(String collection, SubDocType type, Iterable<? extends SubDocument> subDocuments) {
        try {
            SubDocTable table = meta.getCollectionSchema(collection).getSubDocTable(type);

            InsertSetMoreStep<?> insert = null;

            for (SubDocument subDocument : subDocuments) {
                assert subDocument.getType().equals(type);

                SubDocTableRecord record = new SubDocTableRecord(table, subDocTypeBuilderProvider);
                record.setDid(subDocument.getDocumentId());
                record.setIndex(translateSubDocIndexToDatabase(subDocument.getIndex()));
                record.setSubDoc(subDocument);

                if (insert == null) {
                    insert = dsl.insertInto(table).set(record);
                }
                else {
                    insert = insert.newRecord().set(record);
                }
            }

            if (insert != null) {
                insert.execute();
            }
            else {
                assert false : "Call to insertSubdocuments with an empty set of subdocuments";
                LOGGER.warn("Call to insertSubdocuments with an empty set of subdocuments");
            }
        } catch (DataAccessException ex) {
            //TODO: Change exception
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Collection<? extends NamedToroIndex> getIndexes(String collection) {
        try {
            CollectionSchema colSchema = meta.getCollectionSchema(collection);
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
        CollectionSchema colSchema = meta.getCollectionSchema(collection);
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

    protected Integer translateSubDocIndexToDatabase(int index) {
        if (index == 0) {
            return null;
        }
        return index;
    }

    @Override
    public Map<String, Integer> findCollections() {
        Collection<CollectionSchema> collections = meta.getCollectionSchemes();

        Map<String, Integer> result = Maps.newHashMapWithExpectedSize(collections.size());

        for (CollectionSchema colSchema : collections) {
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
    public int delete(String collection, @Nullable QueryCriteria condition, boolean justOne) throws RetryTransactionException {
        CollectionSchema colSchema = meta.getCollectionSchema(collection);
        QueryEvaluator queryEvaluator = new QueryEvaluator(colSchema, databaseInterface);

        Multimap<DocStructure, Integer> didsByStructure = queryEvaluator.evaluateDidsByStructure(condition, dsl);

        return Routines.deleteDocuments(jooqConf, colSchema, didsByStructure, justOne, databaseInterface);
    }

    @Override
    public void dropCollection(String collection) {
        CollectionSchema colSchema = meta.getCollectionSchema(collection);
        Routines.dropCollection(jooqConf, colSchema, databaseInterface);
        meta.dropCollectionSchema(collection);
    }

    @Override
    public NamedDbIndex createIndex(CollectionSchema colSchema, UnnamedDbIndex unnamedDbIndex) {
        return colSchema
                .getIndexStorage()
                .createIndex(dsl, unnamedDbIndex);
    }

    @Override
    public void dropIndex(CollectionSchema colSchema, NamedDbIndex index) {
        colSchema
                .getIndexStorage()
                .dropIndex(dsl, index);
    }

    @Override
    public List<CollectionMetainfo> getCollectionsMetainfo() {
        
        Result<AbstractCollectionsRecord> records = dsl
                .selectFrom(databaseInterface.getCollectionsTable())
                .fetch();
        
        List<CollectionMetainfo> result = Lists.newArrayListWithCapacity(records.size());
        for (AbstractCollectionsRecord record : records) {
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
    public void eventToroIndexRemoved(CollectionSchema colSchema, String indexName) {
        colSchema.getIndexStorage().eventToroIndexRemoved(dsl, indexName);
    }
    
    @Override
    public Integer count(String collection, QueryCriteria query) {
        CollectionSchema colSchema = meta.getCollectionSchema(collection);

        QueryEvaluator queryEvaluator = new QueryEvaluator(colSchema, databaseInterface);

        Set<Integer> dids = queryEvaluator.evaluateDid(
                query,
                dsl,
                0
        );
        
        return dids.size();
    }
    
    @Override
    public List<ToroDocument> readAll(String collection, QueryCriteria query) {
        CollectionSchema colSchema = meta.getCollectionSchema(collection);

        QueryEvaluator queryEvaluator = new QueryEvaluator(colSchema, databaseInterface);

        Set<Integer> dids = queryEvaluator.evaluateDid(
                query,
                dsl,
                0
        );
        
        List<SplitDocument> splitDocs = queryRoutine.execute(getJooqConf(), colSchema, dids.toArray(new Integer[dids.size()]), null, databaseInterface);
        
        List<ToroDocument> toroDocuments = FluentIterable.from(Iterables.transform(
                    splitDocs,
                    d2r.getToDocumentFunction()
                )).toList();
        
        return toroDocuments;
    }

}
