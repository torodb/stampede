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

package com.torodb.torod.db.sql;

import com.google.common.collect.*;
import com.torodb.torod.core.subdocument.SubDocument;
import com.torodb.torod.core.subdocument.SubDocType;
import com.torodb.torod.db.postgresql.meta.CollectionSchema;
import com.torodb.torod.db.postgresql.meta.Routines;
import com.torodb.torod.db.postgresql.meta.TorodbMeta;
import com.torodb.torod.db.postgresql.meta.tables.SubDocTable;
import com.torodb.torod.db.postgresql.meta.tables.records.SubDocTableRecord;
import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.subdocument.structure.DocStructure;
import com.torodb.torod.db.postgresql.converters.StructureConverter;
import com.torodb.torod.db.postgresql.query.QueryEvaluator;
import java.sql.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import javax.inject.Inject;
import org.jooq.*;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

/**
 *
 */
public abstract class AbstractSqlDbConnection implements DbConnection {

    private final TorodbMeta meta;
    private final Configuration jooqConf;
    private final DSLContext dsl;
    private static final Logger LOG
            = Logger.getLogger(AbstractSqlDbConnection.class.getName());

    @Inject
    public AbstractSqlDbConnection(DSLContext dsl, TorodbMeta meta) {
        this.jooqConf = dsl.configuration();
        this.dsl = dsl;
        this.meta = meta;
    }

    protected abstract String getCreateSubDocTypeTableQuery(SubDocTable table, Configuration conf);

    protected abstract String getCreateIndexQuery(SubDocTable table, Field<?> field, Configuration conf);

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
    public void createCollection(String collection) {
        try {
            meta.createCollectionSchema(collection, dsl);

            Routines.createCollectionSchema(jooqConf, collection);
        } catch (DataAccessException ex) {
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

        try {
            Routines.reserveDocIds(
                    jooqConf,
                    collection,
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

    private Integer translateSubDocIndexToDatabase(int index) {
        if (index == 0) {
            return null;
        }
        return index;
    }

    @Override
    public Map<String, Integer> findCollections() {
        Collection<CollectionSchema> collections = meta.getCollectionSchemes();

        Map<String, Integer> result = Maps.newHashMapWithExpectedSize(collections.size());

        for (CollectionSchema schema : collections) {
            String schemaName = schema.getCollection();
            int firstFree = Routines.firstFreeDocId(jooqConf, schemaName);
            result.put(schemaName, firstFree);

            Integer lastUsed = (Integer) getDsl().select(DSL.max(DSL.field("did")))
                    .from(DSL.tableByName(schema.getName(), "root"))
                    .fetchOne().value1();

            if (lastUsed == null) {
                lastUsed = 0;
            }

            if (lastUsed >= firstFree) {
                LOG.log(
                        Level.WARNING, 
                        "Collection {0}.root: last used = {1}. First free = {2}", 
                        new Object[]{
                            schemaName,
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
        CollectionSchema colSchema = meta.getCollectionSchema(collection);
        QueryEvaluator queryEvaluator = new QueryEvaluator(colSchema);

        Multimap<DocStructure, Integer> didsByStructure = queryEvaluator.evaluateDidsByStructure(condition, dsl);

        return Routines.deleteDocuments(jooqConf, colSchema, didsByStructure, justOne);
    }

    @Override
    public Map<Integer, DocStructure> getAllStructures(String collection) throws ImplementationDbException {
        CollectionSchema collectionSchema = meta.getCollectionSchema(collection);

        Field<Integer> structureIdField = DSL.fieldByName(Integer.class, "id");
        Field<String> structureField = DSL.fieldByName(String.class, "_structure");

        Result<Record2<Integer, String>> structures = dsl.select(structureIdField, structureField)
                .from("structures", collectionSchema.getName())
                .fetch();

        StructureConverter converter = new StructureConverter(collectionSchema);
        Map<Integer, DocStructure> result = Maps.newHashMapWithExpectedSize(structures.size());
        for (Record2<Integer, String> record2 : structures) {
            DocStructure structure = converter.from(record2.value2());
            result.put(record2.value1(), structure);
        }

        return result;
    }

    @Override
    public DocStructure getStructure(String collection, int structureId) throws ImplementationDbException {
        CollectionSchema collectionSchema = meta.getCollectionSchema(collection);

        Field<Integer> idField = DSL.fieldByName(Integer.class, "id");
        Field<String> structureField = DSL.fieldByName(String.class, "_structure");

        Record2<Integer, String> found = dsl.select(idField, structureField)
                .from("structures", collectionSchema.getName())
                .where(idField.eq(structureId))
                .fetchOne();

        StructureConverter converter = new StructureConverter(collectionSchema);

        return converter.from(found.value2());
    }
}
