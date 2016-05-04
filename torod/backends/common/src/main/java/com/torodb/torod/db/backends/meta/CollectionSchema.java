package com.torodb.torod.db.backends.meta;

import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Provider;

import org.jooq.DSLContext;
import org.jooq.Meta;
import org.jooq.Schema;
import org.jooq.Sequence;
import org.jooq.Table;
import org.jooq.UDT;
import org.jooq.impl.SchemaImpl;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import com.torodb.torod.core.subdocument.SubDocType;
import com.torodb.torod.db.backends.DatabaseInterface;
import com.torodb.torod.db.backends.converters.StructureConverter;
import com.torodb.torod.db.backends.exceptions.InvalidCollectionSchemaException;
import com.torodb.torod.db.backends.sql.index.IndexManager;
import com.torodb.torod.db.backends.tables.SubDocTable;

public final class CollectionSchema extends SchemaImpl {

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
        this.structuresCache = databaseInterface.createStructuresCache(this, getName(), structureConverter);

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

        this.indexStorage = databaseInterface.createIndexStorage(torodbMeta.getDatabaseName(), this);
        indexStorage.initialize(dsl, schemaName, this);

        this.indexManager = new IndexManager(this, torodbMeta, databaseInterface);
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
