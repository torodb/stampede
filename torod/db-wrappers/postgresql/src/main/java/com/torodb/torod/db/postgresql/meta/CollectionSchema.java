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

package com.torodb.torod.db.postgresql.meta;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import com.torodb.torod.db.postgresql.IdsFilter;
import com.torodb.torod.db.postgresql.converters.StructureConverter;
import com.torodb.torod.db.postgresql.meta.tables.SubDocTable;
import com.torodb.torod.core.subdocument.SubDocType;
import com.torodb.torod.db.sql.index.IndexManager;
import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.jooq.*;
import org.jooq.impl.SchemaImpl;

/**
 *
 */
public final class CollectionSchema extends SchemaImpl {

    private static final long serialVersionUID = 577805060;
    private static final Pattern collectionSchemaPattern = Pattern.compile("col_([_a-z0-9]+)");
    private static final int MIN_VALUE_TABLE_ID = 0;

    private final String collection;
    private StructureConverter structureConverter;
    private final HashBiMap<Integer, SubDocType> typesById;
    private final HashMap<SubDocType, SubDocTable> tables;
    private final AtomicInteger typeIdProvider;
    private final StructuresCache structuresCache;
    private final IndexStorage indexStorage;
    private final IndexManager indexManager;

    CollectionSchema(String collection, DSLContext dsl, TorodbMeta torodbMeta) {
        this(
                getCollectionSchemaName(collection), 
                Collections.<Table<?>>emptyList(), 
                dsl,
                null,
                torodbMeta
        );
    }

    CollectionSchema(Schema schema, DSLContext dsl, DatabaseMetaData jdbcMeta, TorodbMeta torodbMeta) {
        this(
                schema.getName(), 
                schema.getTables(), 
                dsl,
                jdbcMeta,
                torodbMeta
        );
        assert isCollectionSchema(schema);
    }

    private CollectionSchema(
            String schemaName, 
            Iterable<? extends Table> tables, 
            DSLContext dsl,
            DatabaseMetaData jdbcMeta,
            TorodbMeta torodbMeta
    ) {
        super(schemaName);

        this.collection = schemaNameToCollection(schemaName);
        this.typesById = HashBiMap.create();
        this.tables = Maps.newHashMap();
        this.structureConverter = new StructureConverter(this);
        this.structuresCache = new StructuresCache(this, schemaName, structureConverter);

        int maxTypeId = MIN_VALUE_TABLE_ID;

        for (Table<?> table : tables) {
            if (SubDocTable.isSubDocTable(table.getName())) {
                SubDocTable subDocTable = new SubDocTable(
                        table.getName(), 
                        this, 
                        jdbcMeta
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
        this.typeIdProvider = new AtomicInteger(maxTypeId);

        this.structureConverter.initialize();

        this.structuresCache.initialize(dsl, tables);
        
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

    public static boolean isCollectionSchema(Schema schema) {
        return schemaNameToCollection(schema.getName()) != null;
    }
    
    public static boolean isCollectionSchema(String schemaName) {
        return schemaNameToCollection(schemaName) != null;
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

        SubDocTable table = new SubDocTable(this, type, typeId);
        tables.put(type, table);

        return table;
    }

    public StructuresCache getStructuresCache() {
        return structuresCache;
    }

    private static String getCollectionSchemaName(String collection) {
        IdsFilter.filterCollectionName(collection);
        return "col_" + collection;
    }

    /**
     * @param name
     * @return the name of the collection represented by this schema name or null if the given name doesn't represent a
     *         collection
     */
    @Nullable
    public static String schemaNameToCollection(String name) {
        Matcher matcher = collectionSchemaPattern.matcher(name);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
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
}
