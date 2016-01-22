/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General PublicSchema License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General PublicSchema License for more details.
 *
 *     You should have received a copy of the GNU Affero General PublicSchema License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */
package com.torodb.torod.db.backends.meta;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.torodb.torod.core.exceptions.ToroRuntimeException;
import com.torodb.torod.core.pojos.NamedToroIndex;
import com.torodb.torod.core.subdocument.SubDocType;
import com.torodb.torod.db.backends.DatabaseInterface;
import com.torodb.torod.db.backends.converters.StructureConverter;
import com.torodb.torod.db.backends.converters.json.ToroIndexToJsonConverter;
import com.torodb.torod.db.backends.exceptions.InvalidCollectionSchemaException;
import com.torodb.torod.db.backends.sql.index.IndexManager;
import com.torodb.torod.db.backends.sql.index.NamedDbIndex;
import com.torodb.torod.db.backends.sql.index.UnnamedDbIndex;
import com.torodb.torod.db.backends.tables.SubDocTable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.Serializable;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Provider;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.impl.SchemaImpl;
import org.jooq.impl.TableImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class IndexStorage implements Serializable {
    private static final long serialVersionUID = 1L;

    private final ToroIndexTable toroIndexTable;
    private final ToroIndexToJsonConverter indexToJsonConverter;
    private final CollectionSchema colSchema;
    
    private static final Logger LOGGER
            = LoggerFactory.getLogger(IndexStorage.class);
    
    public IndexStorage(String databaseName, CollectionSchema colSchema) {
        this.indexToJsonConverter = new ToroIndexToJsonConverter(
                databaseName,
                colSchema.getCollection()
        );
        this.toroIndexTable = new ToroIndexTable(colSchema, indexToJsonConverter);
        this.colSchema = colSchema;
    }
    
    public void initialize(DSLContext dsl) {
        createIndexTableIfNotExists(dsl);
    }

    private void createIndexTableIfNotExists(DSLContext dsl) {
        boolean tableExists = dsl.select(DSL.count())
                .from("information_schema.tables")
                .where(
                        DSL.field("table_schema").eq(colSchema.getName())
                        .and(DSL.field("table_name").eq(toroIndexTable.getName()))
                )
                .fetchOne(0, int.class) > 0;

        if (!tableExists) {
            Name tableName = DSL.name(colSchema.getName(), toroIndexTable.getName());

            dsl.execute(
                    colSchema.databaseInterface.createIndexesTableStatement(
                            dsl.render(tableName), toroIndexTable.nameColumn.getName(),
                            toroIndexTable.indexColumn.getName()
                    )
            );
        }
    }
    
    public Set<NamedDbIndex> getAllDbIndexes(DSLContext dsl) {
        Connection connection
                = dsl.configuration().connectionProvider().acquire();

        try {
            Set<NamedDbIndex> result = Sets.newHashSet();

            String catalog = connection.getCatalog();
            String schema = colSchema.getName();
            DatabaseMetaData metaData = connection.getMetaData();
            String lastIndexName = null;

            for (SubDocTable table : colSchema.getSubDocTables()) {
                try (ResultSet indexInfo = metaData.getIndexInfo(
                        catalog,
                        schema,
                        table.getName(),
                        false,
                        false
                )) {
                    while (indexInfo.next()) {
                        String indexName = indexInfo.getString("INDEX_NAME");

                        if (!isDbIndexName(indexName)) {
                            LOGGER.trace("{} is not recognized as a db index name", indexInfo);
                            continue;
                        }
                        String columnName = indexInfo.getString("COLUMN_NAME");
                        boolean ascending
                                = indexInfo.getString("ASC_OR_DESC").equals("A");

                        if (lastIndexName != null
                                && lastIndexName.equals(indexName)) {
                            LOGGER.warn("Index {} is recognized as a multiple column "
                                    + "index, which are not supported", lastIndexName);
                            continue;
                        }
                        lastIndexName = indexName;

                        result.add(
                                new NamedDbIndex(
                                        indexName,
                                        new UnnamedDbIndex(
                                                schema,
                                                table.getName(),
                                                columnName,
                                                ascending
                                        )
                                )
                        );
                    }

                    indexInfo.close();
                }
            }

            return result;
        } catch (SQLException ex) {
            throw new ToroRuntimeException(ex);
        } finally {
            dsl.configuration().connectionProvider().release(connection);
        }
    }

    public Set<NamedToroIndex> getAllToroIndexes(DSLContext dsl) {
        Result<Record1<NamedToroIndex>> storedIndexes = dsl
                .select(toroIndexTable.indexColumn)
                .from(toroIndexTable)
                .fetch();

        Set<NamedToroIndex> result = Sets.newHashSet();
        for (Record1<NamedToroIndex> storedIndex : storedIndexes) {
            result.add(storedIndex.value1());
        }
        return result;
    }

    @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
    public void dropIndex(DSLContext dsl, NamedDbIndex index) {
        ConnectionProvider connectionProvider
                = dsl.configuration().connectionProvider();
        Connection connection = connectionProvider.acquire();
        try (Statement st = connection.createStatement()) {
            String query = colSchema.databaseInterface.dropIndexStatement(colSchema.getName(), index.getName());
            st.executeUpdate(query);
        } catch (SQLException ex) {
            throw new ToroRuntimeException(ex);
        } finally {
            connectionProvider.release(connection);
        }
    }

    private boolean isDbIndexName(String indexName) {
        Pattern p = Pattern.compile("t_\\d+_\\S+");
        Matcher matcher = p.matcher(indexName);
        return matcher.find();
    }

    @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
    public NamedDbIndex createIndex(DSLContext dsl, UnnamedDbIndex unnamedDbIndex) {
        String indexName = unnamedDbIndex.getTable() + '_' + unnamedDbIndex.getColumn();
        indexName = colSchema.getDatabaseInterface().escapeIndexName(indexName);
        
        ConnectionProvider connectionProvider
                = dsl.configuration().connectionProvider();
        Connection connection = connectionProvider.acquire();
        try (Statement st = connection.createStatement()) {
            String query = colSchema.databaseInterface.createIndexStatement(
                    indexName, unnamedDbIndex.getSchema(), unnamedDbIndex.getTable(), unnamedDbIndex.getColumn(),
                    unnamedDbIndex.isAscending()
            );
            LOGGER.debug("Creating a db index with query: " + query);
            st.executeUpdate(query);
        } catch (SQLException ex) {
            throw new ToroRuntimeException(ex);
        } finally {
            connectionProvider.release(connection);
        }
        
        return new NamedDbIndex(indexName, unnamedDbIndex);
    }
    
    public void eventToroIndexRemoved(DSLContext dsl, String indexName) {
        dsl.delete(toroIndexTable).where(toroIndexTable.nameColumn.eq(indexName)).execute();
    }

    public void eventToroIndexCreated(DSLContext dsl, NamedToroIndex index) {
        dsl.insertInto(toroIndexTable)
                .set(toroIndexTable.nameColumn, index.getName())
                .set(toroIndexTable.indexColumn, index)
                .execute();
    }

    private static class ToroIndexTable extends TableImpl<Record2<String, NamedToroIndex>> {

        private static final long serialVersionUID = 1L;
        final TableField<Record2<String, NamedToroIndex>, String> nameColumn;
        final TableField<Record2<String, NamedToroIndex>, NamedToroIndex> indexColumn;

        public ToroIndexTable(CollectionSchema colSchema, ToroIndexToJsonConverter indexToJsonConverter) {
            super("indexes", colSchema);
            nameColumn = createField("name", SQLDataType.VARCHAR, this);
            indexColumn = createField("index", SQLDataType.VARCHAR, this, "", indexToJsonConverter);
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash
                    = 53 * hash +
                    (this.nameColumn != null ? this.nameColumn.hashCode() : 0);
            hash
                    = 53 * hash +
                    (this.indexColumn != null ? this.indexColumn.hashCode() : 0);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final ToroIndexTable other = (ToroIndexTable) obj;
            if (this.nameColumn != other.nameColumn &&
                    (this.nameColumn == null ||
                    !this.nameColumn.equals(other.nameColumn))) {
                return false;
            }
            if (this.indexColumn != other.indexColumn &&
                    (this.indexColumn == null ||
                    !this.indexColumn.equals(other.indexColumn))) {
                return false;
            }
            return true;
        }

    }

    /**
     *
     */
    public static final class CollectionSchema extends SchemaImpl {

        private static final long serialVersionUID = 577805060;
        private static final int MIN_VALUE_TABLE_ID = 0;

        private final String collection;
        private StructureConverter structureConverter;
        private final HashBiMap<Integer, SubDocType> typesById;
        private final HashMap<SubDocType, SubDocTable> tables;
        private final AtomicInteger typeIdProvider;
        private final StructuresCache structuresCache;
        private final IndexStorage indexStorage;
        private final IndexManager indexManager;
        private final DatabaseInterface databaseInterface;

        public CollectionSchema(
                @Nonnull String schemName,
                @Nonnull String colName,
                @Nonnull DSLContext dsl,
                @Nonnull TorodbMeta torodbMeta,
                @Nonnull DatabaseInterface databaseInterface,
                @Nonnull Provider<SubDocType.Builder> subDocTypeBuilderProvider
        ) throws InvalidCollectionSchemaException {
            this(
                    schemName,
                    colName,
                    dsl,
                    null,
                    null,
                    torodbMeta,
                    databaseInterface,
                    subDocTypeBuilderProvider
            );
        }

        public CollectionSchema(
                @Nonnull String schemaName,
                @Nonnull String colName,
                @Nonnull DSLContext dsl,
                @Nullable DatabaseMetaData jdbcMeta,
                @Nullable Meta jooqMeta,
                @Nonnull TorodbMeta torodbMeta,
                DatabaseInterface databaseInterface,
                @Nonnull Provider<SubDocType.Builder> subDocTypeBuilderProvider
        ) throws InvalidCollectionSchemaException {
            super(schemaName);

            // TODO: there are a lot of "this" leaks.
            // This has to be fixed, as we are publishing partially initialized objects

            this.collection = colName;
            this.typesById = HashBiMap.create();
            this.tables = Maps.newHashMap();
            this.structureConverter = new StructureConverter(this);
            this.structuresCache = new StructuresCache(this, getName(), structureConverter);

            int maxTypeId = MIN_VALUE_TABLE_ID;

            Iterable<? extends Table> existingTables;
            if (jooqMeta != null) {
                Schema standardSchema = null;
                for (Schema schema : jooqMeta.getSchemas()) {
                    if (schema.getName().equals(schemaName)) {
                        standardSchema = schema;
                        break;
                    }
                }
                if (standardSchema == null) {
                    throw new IllegalStateException(
                            "The collection "+collection+" is associated with schema "
                            + schemaName+" but there is no schema with that name");
                }

                checkCollectionSchema(standardSchema);
                for (Table<?> table : standardSchema.getTables()) {
                    if (SubDocTable.isSubDocTable(table.getName())) {
                        SubDocTable subDocTable = new SubDocTable(
                                table.getName(),
                                this,
                                jdbcMeta,
                                databaseInterface,
                                subDocTypeBuilderProvider
                        );
                        int subDocId = subDocTable.getTypeId();
                        SubDocType type = subDocTable.getSubDocType();
                        this.tables.put(type, subDocTable);
                        this.typesById.put(subDocId, type);

                        if (maxTypeId < subDocId) {
                            maxTypeId = subDocId;
                        }
                    }
                }
                existingTables = standardSchema.getTables();
            }
            else {
                existingTables = Collections.emptySet();
            }
            this.typeIdProvider = new AtomicInteger(maxTypeId);

            this.structureConverter.initialize();

            this.structuresCache.initialize(dsl, existingTables);

            this.databaseInterface = databaseInterface;

            this.indexStorage = new IndexStorage(torodbMeta.getDatabaseName(), this);
            indexStorage.initialize(dsl);

            this.indexManager = new IndexManager(this, torodbMeta);
            indexManager.initialize(
                    indexStorage.getAllDbIndexes(dsl),
                    indexStorage.getAllToroIndexes(dsl),
                    structuresCache
            );
        }

        public IndexManager getIndexManager() {
            return indexManager;
        }

        public IndexStorage getIndexStorage() {
            return indexStorage;
        }

        public static void checkCollectionSchema(Schema schema) throws InvalidCollectionSchemaException {
            //TODO: improve checks
        }

        /**
         *
         * @param typeId
         * @return
         * @throws IllegalArgumentException if there is no type registered with the given type id
         */
        public SubDocTable getSubDocTable(int typeId) throws IllegalArgumentException {
            return getSubDocTable(getSubDocType(typeId));
        }

        /**
         *
         * @param type
         * @return
         * @throws IllegalArgumentException if there is no prepared table that represents the given type
         */
        public SubDocTable getSubDocTable(SubDocType type) throws IllegalArgumentException {
            SubDocTable table = tables.get(type);
            if (table == null) {
                throw new IllegalArgumentException("There is no table that represents type " + type);
            }
            return table;
        }

        /**
         *
         * @param type
         * @return
         * @throws IllegalArgumentException if the given type is not registered
         */
        public int getTypeId(SubDocType type) throws IllegalArgumentException {
            BiMap<SubDocType, Integer> idsByType = typesById.inverse();

            Integer result = idsByType.get(type);
            if (result == null) {
                throw new IllegalArgumentException("Type " + type + " is not registered");
            }
            return result;
        }

        /**
         *
         * @param typeId
         * @return
         * @throws IllegalArgumentException if there is not type associated with the given id
         */
        public SubDocType getSubDocType(int typeId) throws IllegalArgumentException {
            SubDocType type = typesById.get(typeId);
            if (type == null) {
                throw new IllegalArgumentException("There is no type registered with the id " + typeId);
            }
            return type;
        }

        public boolean existsSubDocTable(SubDocType type) {
            return tables.containsKey(type);
        }

        /**
         *
         * @param type
         * @return
         * @throws IllegalArgumentException if the given type is already represented by a table
         */
        public SubDocTable prepareSubDocTable(SubDocType type) throws IllegalArgumentException {
            if (tables.containsKey(type)) {
                throw new IllegalArgumentException("There is already a table that represents type " + type);
            }
            int typeId = typeIdProvider.incrementAndGet();
            typesById.put(typeId, type);

            SubDocTable table = new SubDocTable(this, type, typeId, databaseInterface);
            tables.put(type, table);

            return table;
        }

        public StructuresCache getStructuresCache() {
            return structuresCache;
        }

        public StructureConverter getStructureConverter() {
            return structureConverter;
        }

        public String getCollection() {
            return collection;
        }

        @Override
        public final List<Sequence<?>> getSequences() {
            return Collections.<Sequence<?>>emptyList();
        }

        @Override
        public final List<Table<?>> getTables() {
            List<Table<?>> result = new ArrayList();
            result.addAll(getSubDocTables());

            return result;
        }

        @Override
        public final List<UDT<?>> getUDTs() {
            List result = new ArrayList();
            result.addAll(getUDTs0());
            return result;
        }

        private List<UDT<?>> getUDTs0() {
            return Collections.emptyList();
        }

        public Collection<SubDocTable> getSubDocTables() {
            return Collections.unmodifiableCollection(tables.values());
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return super.equals(obj);
        }

        public DatabaseInterface getDatabaseInterface() {
            return databaseInterface;
        }
    }
}
