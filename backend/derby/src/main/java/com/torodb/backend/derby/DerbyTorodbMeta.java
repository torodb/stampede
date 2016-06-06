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

package com.torodb.backend.derby;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.jooq.DSLContext;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.Meta;
import org.jooq.Schema;
import org.jooq.Table;

import com.torodb.backend.DatabaseInterface;
import com.torodb.backend.exceptions.InvalidDatabaseException;
import com.torodb.backend.exceptions.InvalidDatabaseSchemaException;
import com.torodb.backend.meta.TorodbMeta;
import com.torodb.backend.meta.TorodbSchema;
import com.torodb.backend.tables.DocPartHelper;
import com.torodb.backend.tables.MetaCollectionTable;
import com.torodb.backend.tables.MetaDatabaseTable;
import com.torodb.backend.tables.MetaDocPartTable;
import com.torodb.backend.tables.MetaFieldTable;
import com.torodb.backend.tables.records.MetaCollectionRecord;
import com.torodb.backend.tables.records.MetaDatabaseRecord;
import com.torodb.backend.tables.records.MetaDocPartRecord;
import com.torodb.backend.tables.records.MetaFieldRecord;
import com.torodb.core.TableRef;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.ImmutableMetaField;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;

/**
 *
 */
public class DerbyTorodbMeta implements TorodbMeta {

    private final DatabaseInterface databaseInterface;
    private final ImmutableMetaSnapshot metaSnapshot;

    DerbyTorodbMeta(
            DSLContext dsl,
            DatabaseInterface databaseInterface)
    throws SQLException, IOException, InvalidDatabaseException {
        this.databaseInterface = databaseInterface;

        Meta jooqMeta = dsl.meta();
        Connection conn = dsl.configuration().connectionProvider().acquire();

        TorodbSchema.TORODB.checkOrCreate(dsl, jooqMeta, databaseInterface);
        metaSnapshot = loadMetaSnapshot(dsl, jooqMeta);
        
        dsl.configuration().connectionProvider().release(conn);
    }
    
    public ImmutableMetaSnapshot getCurrentMetaSnapshot() {
        return metaSnapshot;
    }
    
    private ImmutableMetaSnapshot loadMetaSnapshot(
            DSLContext dsl,
            Meta jooqMeta) throws InvalidDatabaseSchemaException {
        
        MetaDatabaseTable<?> metaDatabaseTable = databaseInterface.getMetaDatabaseTable();
        List<MetaDatabaseRecord> records
                = dsl.select(metaDatabaseTable.NAME, metaDatabaseTable.IDENTIFIER)
                    .from(metaDatabaseTable)
                    .fetchInto(MetaDatabaseRecord.class);
        
        ImmutableMetaSnapshot.Builder metaSnapshotBuilder = new ImmutableMetaSnapshot.Builder();
        for (MetaDatabaseRecord databaseRecord : records) {
            ImmutableMetaDatabase.Builder metaDatabaseBuilder = new ImmutableMetaDatabase.Builder(
                    databaseRecord.getName(), databaseRecord.getIdentifier());
            
            String database = databaseRecord.getName();
            String schemaName = databaseRecord.getIdentifier();
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
            Map<String, MetaDocPartRecord> docParts = dsl
                    .select(docPartTable.COLLECTION, docPartTable.TABLE_REF, docPartTable.IDENTIFIER)
                    .from(docPartTable)
                    .where(docPartTable.DATABASE.eq(database))
                    .fetchMap(docPartTable.IDENTIFIER, MetaDocPartRecord.class);
            List<MetaFieldRecord> fields = dsl
                    .select(fieldTable.COLLECTION, fieldTable.TABLE_REF, fieldTable.NAME, 
                            docPartTable.IDENTIFIER, fieldTable.IDENTIFIER, fieldTable.TYPE)
                    .from(docPartTable)
                    .naturalJoin(fieldTable)
                    .where(fieldTable.DATABASE.eq(database))
                    .fetchInto(MetaFieldRecord.class);
            
            Iterable<? extends Table<?>> existingTables = standardSchema.getTables();
            for (MetaCollectionRecord collection : collections) {
                MetaDocPartRecord<?> rootMetaDocPartRecord = null;
                for (Map.Entry<String, MetaDocPartRecord> container : docParts.entrySet()) {
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
                
                for (Map.Entry<String, MetaDocPartRecord> container : docParts.entrySet()) {
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
            
            metaSnapshotBuilder.add(metaDatabaseBuilder.build());
        }
        
        return metaSnapshotBuilder.build();
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
    
    private boolean containsField(Field<?> existingField, String tableName, Iterable<MetaFieldRecord> fields) {
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
}
