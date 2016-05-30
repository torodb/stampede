package com.torodb.poc.backend.meta;

import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.jooq.DSLContext;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.Meta;
import org.jooq.Record6;
import org.jooq.Result;
import org.jooq.Schema;
import org.jooq.Sequence;
import org.jooq.Table;
import org.jooq.UDT;
import org.jooq.impl.SchemaImpl;

import com.google.common.collect.HashBasedTable;
import com.torodb.poc.backend.DatabaseInterface;
import com.torodb.poc.backend.exceptions.InvalidDatabaseSchemaException;
import com.torodb.poc.backend.mocks.Path;
import com.torodb.poc.backend.mocks.PathDocStructure;
import com.torodb.poc.backend.tables.CollectionTable;
import com.torodb.poc.backend.tables.ContainerTable;
import com.torodb.poc.backend.tables.FieldTable;
import com.torodb.poc.backend.tables.PathDocHelper;
import com.torodb.poc.backend.tables.PathDocTable;
import com.torodb.poc.backend.tables.RootDocTable;
import com.torodb.poc.backend.tables.records.CollectionRecord;
import com.torodb.poc.backend.tables.records.ContainerRecord;
import com.torodb.poc.backend.tables.records.FieldRecord;

public final class DatabaseSchema extends SchemaImpl {

    private static final long serialVersionUID = 577805060;

    private final String database;
    private final Map<String, RootDocTable> roots;
    private final com.google.common.collect.Table<String, Path, PathDocTable> containers;
    private final DatabaseInterface databaseInterface;

    public DatabaseSchema(
            @Nonnull String database,
            @Nonnull String schemName,
            @Nonnull DSLContext dsl,
            @Nonnull DatabaseInterface databaseInterface
    ) throws InvalidDatabaseSchemaException {
        this(
                database,
                schemName,
                dsl,
                null,
                null,
                databaseInterface
        );
    }

    public DatabaseSchema(
            @Nonnull String database,
            @Nonnull String schemaName,
            @Nonnull DSLContext dsl,
            @Nullable DatabaseMetaData jdbcMeta,
            @Nullable Meta jooqMeta,
            DatabaseInterface databaseInterface
    ) throws InvalidDatabaseSchemaException {
        super(schemaName);

        // TODO: there are a lot of "this" leaks.
        // This has to be fixed, as we are publishing partially initialized objects

        this.database = database;
        this.roots = new HashMap<>();
        this.containers = HashBasedTable.create();

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
                        "The database "+database+" is associated with schema "
                        + schemaName+" but there is no schema with that name");
            }

            checkDatabaseSchema(standardSchema);
            
            CollectionTable<?> collectionTable = databaseInterface.getCollectionTable();
            ContainerTable<?> containerTable = databaseInterface.getContainerTable();
            FieldTable<?> fieldTable = databaseInterface.getFieldTable();
            Map<String, CollectionRecord> collections = dsl
                    .select(collectionTable.NAME, collectionTable.TABLE)
                    .from(collectionTable)
                    .where(collectionTable.DATABASE.eq(database))
                    .fetchMap(collectionTable.TABLE, CollectionRecord.class);
            Map<String, ContainerRecord> containers = dsl
                    .select(containerTable.COLLECTION, containerTable.TABLE, containerTable.PARENT_TABLE)
                    .from(containerTable)
                    .where(containerTable.DATABASE.eq(database))
                    .fetchMap(containerTable.TABLE, ContainerRecord.class);
            Map<String, Result<Record6<String, String, String, String, String, String>>> fields = dsl
                    .select(fieldTable.COLLECTION, fieldTable.PATH, fieldTable.NAME, 
                            containerTable.TABLE, fieldTable.COLUMN_NAME, fieldTable.COLUMN_TYPE)
                    .from(containerTable)
                    .naturalJoin(fieldTable)
                    .where(fieldTable.DATABASE.eq(database))
                    .fetchGroups(containerTable.TABLE);
            
            Iterable<? extends Table<?>> existingTables = standardSchema.getTables();
            for (Map.Entry<String, CollectionRecord> collection : collections.entrySet()) {
                if (!existsTable(collection.getValue().getTableName(), existingTables)) {
                    throw new InvalidDatabaseSchemaException(schemaName, "Collection "+collection.getValue().getName()
                            +" in database "+database
                            +" is associated with table "+collection.getValue().getTableName()
                            +" but there is no table with that name in schema "+schemaName);
                }
            }
            for (Map.Entry<String, ContainerRecord> container : containers.entrySet()) {
                if (!existsTable(container.getValue().getTableName(), existingTables)) {
                    throw new InvalidDatabaseSchemaException(schemaName, "Container "+container.getValue().getPath()
                            +" in database "+database
                            +" is associated with table "+container.getValue().getTableName()
                            +" but there is no table with that name in schema "+schemaName);
                }
            }
            for (Map.Entry<String, Result<Record6<String, String, String, String, String, String>>> tableFields : fields.entrySet()) {
                for (Record6<String, String, String, String, String, String> field : tableFields.getValue()) {
                    if (!existsColumn(field.value4(), field.value5(), existingTables)) {
                        throw new InvalidDatabaseSchemaException(schemaName, "Field "+field.value2()+"."
                                +field.value3()+" in database "+database+" is associated with field "+field.field4()
                                +"."+field.field5()+" but there is no field with that name in table "
                                +schemaName+"."+field.field4());
                    }
                    if (!existsColumnWithType(field.value4(), field.value5(), 
                            databaseInterface.getDataType(field.value6()), existingTables)) {
                        throw new InvalidDatabaseSchemaException(schemaName, "Field "+field.value2()+"."
                                +field.value3()+" in database "+database+" is associated with field "+field.field4()
                                +"."+field.field5()+" and type "+databaseInterface.getDataType(field.value6()).getTypeName()
                                +" but the field "+schemaName+"."+field.field4()+"."+field.field5()
                                +" has a different type "+getColumnType(field.value4(), field.value5(), existingTables).getTypeName());
                    }
                }
            }
            
            PathDocHelper pathDocHelper = new PathDocHelper(databaseInterface);
            for (Table<?> table : existingTables) {
                if (!collections.containsKey(table) && !containers.containsKey(table.getName())) {
                    throw new InvalidDatabaseSchemaException(schemaName, "Table "+schemaName+"."+table.getName()
                            +" has no container associated for database "+database);
                }
                if (containers.containsKey(table.getName())) {
                    ContainerRecord container = containers.get(table.getName());
                    Result<Record6<String, String, String, String, String, String>> tableFields = fields.get(table.getName());
                    for (Field<?> existingField : table.fields()) {
                        if (pathDocHelper.isSpecialColumn(existingField.getName())) {
                            continue;
                        }
                        if (!containsField(existingField, tableFields)) {
                            throw new InvalidDatabaseSchemaException(schemaName, "Column "+schemaName+"."+table.getName()
                            +"."+existingField.getName()+" has no field associated for database "+database);
                        }
                    }
                    Path path = Path.fromString(container.getPath());
                    PathDocStructure pathDocStructure = pathDocStructureFromTableFields(tableFields);
                    PathDocTable pathDocTable = new PathDocTable(
                            database,
                            container.getCollection(),
                            path,
                            this,
                            container.getTableName(),
                            container.getParentTableName(),
                            pathDocStructure,
                            databaseInterface
                    );
                    this.containers.put(container.getCollection(), path, pathDocTable);
                }
                if (collections.containsKey(table.getName())) {
                    
                }
            }
        }

        this.databaseInterface = databaseInterface;
    }
    
    private PathDocStructure pathDocStructureFromTableFields(Result<Record6<String, String, String, String, String, String>> tableFields) {
        List<FieldRecord> tableFieldRecords = new ArrayList<>();
        for (Record6<String, String, String, String, String, String> tableFieldRecord : tableFieldRecords) {
            FieldRecord field = databaseInterface.getFieldTable().newRecord();
            field.setDatabase(database);
            field.setCollection(tableFieldRecord.value1());
            field.setPath(tableFieldRecord.value2());
            field.setName(tableFieldRecord.value3());
            field.setColumnName(tableFieldRecord.value5());
            field.setColumnType(tableFieldRecord.value6());
            tableFieldRecords.add(field);
        }
        return PathDocStructure.fromTableFields(tableFieldRecords);
    }

    private boolean existsTable(String tableName, Iterable<? extends Table<?>> tables) {
        for (Table<?> table : tables) {
            if (table.getName().equals(tableName)) {
                return true;
            }
        }
        return false;
    }

    private boolean existsColumn(String tableName, String columnName, Iterable<? extends Table<?>> tables) {
        for (Table<?> table : tables) {
            if (table.getName().equals(tableName)) {
                for (Field<?> field : table.fields()) {
                    if (field.getName().equals(columnName)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private boolean existsColumnWithType(String tableName, String columnName, DataType<?> columnType, Iterable<? extends Table<?>> tables) {
        for (Table<?> table : tables) {
            if (table.getName().equals(tableName)) {
                for (Field<?> field : table.fields()) {
                    if (field.getName().equals(columnName) &&
                            field.getDataType().equals(columnType)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private DataType<?> getColumnType(String tableName, String columnName, Iterable<? extends Table<?>> tables) {
        for (Table<?> table : tables) {
            if (table.getName().equals(tableName)) {
                for (Field<?> field : table.fields()) {
                    if (field.getName().equals(columnName)) {
                        return field.getDataType();
                    }
                }
            }
        }
        return null;
    }
    
    private boolean containsField(Field<?> existingField, Iterable<Record6<String, String, String, String, String, String>> tableFields) {
        for (Record6<String, String, String, String, String, String> field : tableFields) {
            if (existingField.getName().equals(field.value5())) {
                return true;
            }
        }
        return false;
    }
    
    public static void checkDatabaseSchema(Schema schema) throws InvalidDatabaseSchemaException {
        //TODO: improve checks
    }

    /**
     *
     * @param path
     * @return
     * @throws IllegalArgumentException if there is no table registered with the given path
     */
    public PathDocTable getPathDocTable(String collection, String path) throws IllegalArgumentException {
        PathDocTable table = containers.get(collection, path);
        if (table == null) {
            throw new IllegalArgumentException("There is no table that represents path " + path + " for collection " + collection);
        }
        return table;
    }

    public boolean existsPathDocTable(String collection, String path) {
        return containers.contains(collection, path);
    }

    /**
     *
     * @param path
     * @return
     * @throws IllegalArgumentException if the given type is already represented by a table
     */
    public PathDocTable preparePathDocTable(String collection, Path path, String tableName, 
            String parentTableName, PathDocStructure pathDocStructure) throws IllegalArgumentException {
        if (containers.contains(collection, path)) {
            throw new IllegalArgumentException("There is no table that represents path " + path + " for collection " + collection);
        }

        PathDocTable table = new PathDocTable(database, collection, path, this, 
                tableName, parentTableName, pathDocStructure, databaseInterface);
        containers.put(collection, path, table);

        return table;
    }

    public String getDatabase() {
        return database;
    }

    @Override
    public final List<Sequence<?>> getSequences() {
        return Collections.<Sequence<?>>emptyList();
    }

    @Override
    public final List<Table<?>> getTables() {
        List<Table<?>> result = new ArrayList<>();
        result.addAll(getPathDocTables());

        return result;
    }

    @Override
    public final List<UDT<?>> getUDTs() {
        List<UDT<?>> result = new ArrayList<>();
        result.addAll(getUDTs0());
        return result;
    }

    private List<UDT<?>> getUDTs0() {
        return Collections.emptyList();
    }

    public Collection<PathDocTable> getPathDocTables() {
        return Collections.unmodifiableCollection(containers.values());
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
