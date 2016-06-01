package com.torodb.poc.backend.meta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.jooq.DSLContext;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.Meta;
import org.jooq.Schema;
import org.jooq.Sequence;
import org.jooq.Table;
import org.jooq.UDT;
import org.jooq.impl.SchemaImpl;

import com.torodb.core.TableRef;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.ImmutableMetaField;
import com.torodb.poc.backend.DatabaseInterface;
import com.torodb.poc.backend.exceptions.InvalidDatabaseSchemaException;
import com.torodb.poc.backend.tables.DocPartHelper;
import com.torodb.poc.backend.tables.MetaCollectionTable;
import com.torodb.poc.backend.tables.MetaDocPartTable;
import com.torodb.poc.backend.tables.MetaFieldTable;
import com.torodb.poc.backend.tables.records.MetaCollectionRecord;
import com.torodb.poc.backend.tables.records.MetaDocPartRecord;
import com.torodb.poc.backend.tables.records.MetaFieldRecord;

public final class DatabaseSchema extends SchemaImpl {

    private static final long serialVersionUID = 577805060;

    private final String database;
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
            @Nullable Meta jooqMeta,
            @Nullable ImmutableMetaDatabase.Builder metaDatabaseBuilder,
            DatabaseInterface databaseInterface
    ) throws InvalidDatabaseSchemaException {
        super(schemaName);

        // TODO: there are a lot of "this" leaks.
        // This has to be fixed, as we are publishing partially initialized objects

        this.database = database;

        if (jooqMeta != null && metaDatabaseBuilder != null) {
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
            
            MetaCollectionTable<?> collectionTable = databaseInterface.getMetaCollectionTable();
            MetaDocPartTable<?, ?> docPartTable = databaseInterface.getMetaDocPartTable();
            MetaFieldTable<?, ?> fieldTable = databaseInterface.getMetaFieldTable();
            List<MetaCollectionRecord> collections = dsl
                    .select(collectionTable.NAME)
                    .from(collectionTable)
                    .where(collectionTable.DATABASE.eq(database))
                    .fetchInto(MetaCollectionRecord.class);
            Map<String, MetaDocPartRecord<?>> docParts = dsl
                    .select(docPartTable.COLLECTION, docPartTable.TABLE_REF, docPartTable.IDENTIFIER)
                    .from(docPartTable)
                    .where(docPartTable.DATABASE.eq(database))
                    .fetchMap(docPartTable.IDENTIFIER, MetaDocPartRecord.class);
            List<MetaFieldRecord<?>> fields = dsl
                    .select(fieldTable.COLLECTION, fieldTable.TABLE_REF, fieldTable.NAME, 
                            docPartTable.IDENTIFIER, fieldTable.IDENTIFIER, fieldTable.TYPE)
                    .from(docPartTable)
                    .naturalJoin(fieldTable)
                    .where(fieldTable.DATABASE.eq(database))
                    .fetchInto(MetaFieldRecord.class);
            
            Iterable<? extends Table<?>> existingTables = standardSchema.getTables();
            for (MetaCollectionRecord collection : collections) {
                MetaDocPartRecord<?> rootMetaDocPartRecord = null;
                for (Map.Entry<String, MetaDocPartRecord<?>> container : docParts.entrySet()) {
                    if (!container.getValue().getCollection().equals(collection.getName())) {
                        continue;
                    }
                    
                    TableRef tableRef = databaseInterface.toTableRef(container.getValue().getTableRef());
                    if (tableRef.isRoot()) {
                        rootMetaDocPartRecord = container.getValue();
                        break;
                    }
                }
                if (rootMetaDocPartRecord == null) {
                    throw new InvalidDatabaseSchemaException(schemaName, "Collection "+collection.getName()
                            +" in database "+database
                            +" has no root table in meta data");
                }
                ImmutableMetaCollection.Builder metaCollectionBuilder = 
                        new ImmutableMetaCollection.Builder(
                                collection.getName(), 
                                rootMetaDocPartRecord.getIdentifier());
                
                for (Map.Entry<String, MetaDocPartRecord<?>> container : docParts.entrySet()) {
                    if (!container.getValue().getCollection().equals(collection.getName())) {
                        continue;
                    }
                    
                    TableRef tableRef = databaseInterface.toTableRef(container.getValue().getTableRef());
                    ImmutableMetaDocPart.Builder metaDocPartBuilder = new ImmutableMetaDocPart.Builder(
                            tableRef, 
                            container.getValue().getIdentifier());
                    if (!existsTable(container.getValue().getIdentifier(), existingTables)) {
                        throw new InvalidDatabaseSchemaException(schemaName, "Container "+databaseInterface.toTableRef(container.getValue().getTableRef())
                                +" in database "+database
                                +" is associated with table "+container.getValue().getIdentifier()
                                +" but there is no table with that name in schema "+schemaName);
                    }
                    for (MetaFieldRecord<?> field : fields) {
                        TableRef fieldTableRef = databaseInterface.toTableRef(field.getTableRef());
                        if (!tableRef.equals(fieldTableRef)) {
                            continue;
                        }
                        
                        ImmutableMetaField metaField = new ImmutableMetaField(
                                field.getName(), 
                                field.getIdentifier(), 
                                FieldType.valueOf(field.getType()));
                        
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
                        metaDocPartBuilder.add(metaField);
                    }
                    metaCollectionBuilder.add(metaDocPartBuilder.build());
                }
                
                metaDatabaseBuilder.add(metaCollectionBuilder.build());
            }
            
            DocPartHelper docPartHelper = new DocPartHelper(databaseInterface);
            for (Table<?> table : existingTables) {
                if (!docParts.containsKey(table.getName())) {
                    throw new InvalidDatabaseSchemaException(schemaName, "Table "+schemaName+"."+table.getName()
                            +" has no container associated for database "+database);
                }
                if (docParts.containsKey(table.getName())) {
                    for (Field<?> existingField : table.fields()) {
                        if (docPartHelper.isSpecialColumn(existingField.getName())) {
                            continue;
                        }
                        if (!containsField(existingField, table.getName(), fields)) {
                            throw new InvalidDatabaseSchemaException(schemaName, "Column "+schemaName+"."+table.getName()
                            +"."+existingField.getName()+" has no field associated for database "+database);
                        }
                    }
                }
            }
        }

        this.databaseInterface = databaseInterface;
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
    
    private boolean containsField(Field<?> existingField, String tableName, Iterable<MetaFieldRecord<?>> fields) {
        for (MetaFieldRecord<?> field : fields) {
            if (field.getIdentifier().equals(tableName) &&
                    existingField.getName().equals(field.getIdentifier())) {
                return true;
            }
        }
        return false;
    }
    
    public static void checkDatabaseSchema(Schema schema) throws InvalidDatabaseSchemaException {
        //TODO: improve checks
    }

    public String getDatabase() {
        return database;
    }

    @Override
    public final List<Sequence<?>> getSequences() {
        return Collections.<Sequence<?>>emptyList();
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
