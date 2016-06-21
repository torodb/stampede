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

package com.torodb.backend.meta;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Meta;
import org.jooq.Result;
import org.jooq.Table;

import com.torodb.backend.SqlInterface;
import com.torodb.backend.exceptions.InvalidDatabaseException;
import com.torodb.backend.exceptions.InvalidDatabaseSchemaException;
import com.torodb.backend.interfaces.ErrorHandlerInterface.Context;
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
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.ImmutableMetaField;
import com.torodb.core.transaction.metainf.ImmutableMetaScalar;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MetaSnapshot;

/**
 *
 */
public class TorodbMeta {

    private final SqlInterface sqlInterface;
    private final ImmutableMetaSnapshot metaSnapshot;
    private final Map<String,Map<String,Map<TableRef,Integer>>> lastIds;

    @Inject
    public TorodbMeta(
            TableRefFactory tableRefFactory,
            SqlInterface sqlInterface)
    throws InvalidDatabaseException {
        this.sqlInterface = sqlInterface;

        try (Connection connection = sqlInterface.createSystemConnection()) {
            DSLContext dsl = sqlInterface.createDSLContext(connection);
            Meta jooqMeta = dsl.meta();
    
            TorodbSchema.TORODB.checkOrCreate(dsl, jooqMeta, sqlInterface);
            metaSnapshot = loadMetaSnapshot(dsl, jooqMeta, tableRefFactory);
            lastIds = loadRowIds(dsl, metaSnapshot);
        } catch(SQLException sqlException) {
            sqlInterface.handleRollbackException(Context.unknown, sqlException);
            
            throw new InvalidDatabaseException(sqlException);
        }
    }
    
    public ImmutableMetaSnapshot getCurrentMetaSnapshot() {
        return metaSnapshot;
    }
    
    public Map<String,Map<String,Map<TableRef,Integer>>> getLastIds() {
    	return lastIds;
    }
    
    private ImmutableMetaSnapshot loadMetaSnapshot(
            DSLContext dsl,
            Meta jooqMeta,
            TableRefFactory tableRefFactory) throws InvalidDatabaseSchemaException {
        
        MetaDatabaseTable<MetaDatabaseRecord> metaDatabaseTable = sqlInterface.getMetaDatabaseTable();
        Result<MetaDatabaseRecord> records
                = dsl.selectFrom(metaDatabaseTable)
                    .fetch();
        
        MetaCollectionTable<MetaCollectionRecord> collectionTable = sqlInterface.getMetaCollectionTable();
        MetaDocPartTable<Object, MetaDocPartRecord<Object>> docPartTable = sqlInterface.getMetaDocPartTable();
        MetaFieldTable<Object, MetaFieldRecord<Object>> fieldTable = sqlInterface.getMetaFieldTable();
        MetaScalarTable<Object, MetaScalarRecord<Object>> scalarTable = sqlInterface.getMetaScalarTable();

        ImmutableMetaSnapshot.Builder metaSnapshotBuilder = new ImmutableMetaSnapshot.Builder();
        for (MetaDatabaseRecord databaseRecord : records) {
        	String database = databaseRecord.getName();
        	String schemaName = databaseRecord.getIdentifier();
        	
            ImmutableMetaDatabase.Builder metaDatabaseBuilder = new ImmutableMetaDatabase.Builder(
            		database, schemaName);
            
            SchemaValidator schemaValidator = new SchemaValidator(jooqMeta, schemaName, database);
            
            Result<MetaCollectionRecord> collections = dsl
                    .selectFrom(collectionTable)
                    .where(collectionTable.DATABASE.eq(database))
                    .fetch();
            
            
            for (MetaCollectionRecord collection : collections) {
            	String collectionName = collection.getName();
            	
            	ImmutableMetaCollection.Builder metaCollectionBuilder = 
            			new ImmutableMetaCollection.Builder(collectionName, collection.getIdentifier());
            	
                List<MetaDocPartRecord<Object>> docParts = dsl
                        .selectFrom(docPartTable)
                        .where(docPartTable.DATABASE.eq(database)
                            .and(docPartTable.COLLECTION.eq(collectionName)))
                        .fetch();
                
                for (MetaDocPartRecord<Object> docPart : docParts) {
                    if (!docPart.getCollection().equals(collectionName)) {
                        continue;
                    }
                    String docPartIdentifier = docPart.getIdentifier();
                    
                    TableRef tableRef = docPart.getTableRefValue(tableRefFactory);
                    ImmutableMetaDocPart.Builder metaDocPartBuilder = new ImmutableMetaDocPart.Builder(
                            tableRef, docPartIdentifier);
                    
                    if (!schemaValidator.existsTable(docPartIdentifier)) {
                        throw new InvalidDatabaseSchemaException(schemaName, "Doc part "+tableRef
                                +" in database "+database
                                +" is associated with table "+docPartIdentifier
                                +" but there is no table with that name in schema "+schemaName);
                    }
                    List<MetaFieldRecord<Object>> fields = dsl
                            .selectFrom(fieldTable)
                            .where(fieldTable.DATABASE.eq(database)
                                .and(fieldTable.COLLECTION.eq(collectionName))
                                .and(fieldTable.TABLE_REF.eq(docPart.getTableRef())))
                            .fetch();
                    
                    for (MetaFieldRecord<?> field : fields) {
                        TableRef fieldTableRef = field.getTableRefValue(tableRefFactory);
                        if (!tableRef.equals(fieldTableRef)) {
                            continue;
                        }
                        
                        ImmutableMetaField metaField = new ImmutableMetaField(
                                field.getName(), 
                                field.getIdentifier(), 
                                field.getType());
                        
                        if (!schemaValidator.existsColumn(docPartIdentifier, field.getIdentifier())) {
                            throw new InvalidDatabaseSchemaException(schemaName, "Field "+field.getCollection()+"."
                                    +field.getTableRefValue(tableRefFactory)+"."+field.getName()+" of type "+field.getType()
                                    +" in database "+database+" is associated with field "+field.getIdentifier()
                                    +" but there is no field with that name in table "
                                    +schemaName+"."+docPartIdentifier);
                        }
                        if (!schemaValidator.existsColumnWithType(docPartIdentifier, field.getIdentifier(), 
                                sqlInterface.getDataType(field.getType()))) {
                            //TODO: some types can not be recognized using meta data
                            //throw new InvalidDatabaseSchemaException(schemaName, "Field "+field.getCollection()+"."
                            //        +field.getTableRefValue()+"."+field.getName()+" in database "+database+" is associated with field "+field.getIdentifier()
                            //        +" and type "+sqlInterface.getDataType(field.getType()).getTypeName()
                            //        +" but the field "+schemaName+"."+docPartIdentifier+"."+field.getIdentifier()
                            //        +" has a different type "+getColumnType(docPartIdentifier, field.getIdentifier(), existingTables).getTypeName());
                        }
                        metaDocPartBuilder.add(metaField);
                    }
                    List<MetaScalarRecord<Object>> scalars = dsl
                            .selectFrom(scalarTable)
                            .where(scalarTable.DATABASE.eq(database)
                                .and(scalarTable.COLLECTION.eq(collectionName))
                                .and(scalarTable.TABLE_REF.eq(docPart.getTableRef())))
                            .fetch();

                    for (MetaScalarRecord<?> scalar : scalars) {
                        TableRef fieldTableRef = scalar.getTableRefValue(tableRefFactory);
                        if (!tableRef.equals(fieldTableRef)) {
                            continue;
                        }
                        
                        ImmutableMetaScalar metaScalar = new ImmutableMetaScalar(
                                scalar.getIdentifier(), 
                                scalar.getType());
                        
                        if (!schemaValidator.existsColumn(docPartIdentifier, scalar.getIdentifier())) {
                            throw new InvalidDatabaseSchemaException(schemaName, "Scalar "+scalar.getCollection()+"."
                                    +scalar.getTableRefValue(tableRefFactory)+" of type "+scalar.getType()
                                    +" in database "+database+" is associated with scalar "+scalar.getIdentifier()
                                    +" but there is no scalar with that name in table "
                                    +schemaName+"."+docPartIdentifier);
                        }
                        if (!schemaValidator.existsColumnWithType(docPartIdentifier, scalar.getIdentifier(), 
                                sqlInterface.getDataType(scalar.getType()))) {
                            //TODO: some types can not be recognized using meta data
                            //throw new InvalidDatabaseSchemaException(schemaName, "Scalar "+scalar.getCollection()+"."
                            //        +scalar.getTableRefValue()+"."+scalar.getName()+" in database "+database+" is associated with scalar "+scalar.getIdentifier()
                            //        +" and type "+sqlInterface.getDataType(scalar.getType()).getTypeName()
                            //        +" but the scalar "+schemaName+"."+docPartIdentifier+"."+scalar.getIdentifier()
                            //        +" has a different type "+getColumnType(docPartIdentifier, scalar.getIdentifier(), existingTables).getTypeName());
                        }
                        metaDocPartBuilder.add(metaScalar);
                    }

                    metaCollectionBuilder.add(metaDocPartBuilder.build());
                }
                metaDatabaseBuilder.add(metaCollectionBuilder.build());
            }
            metaSnapshotBuilder.add(metaDatabaseBuilder.build());
            
            
	        Map<String, MetaDocPartRecord<Object>> docParts = dsl
	        		.selectFrom(docPartTable)
	        		.where(docPartTable.DATABASE.eq(database))
	        		.fetchMap(docPartTable.IDENTIFIER);
            List<MetaFieldRecord<Object>> fields = dsl
                    .selectFrom(fieldTable)
                    .where(fieldTable.DATABASE.eq(database))
                    .fetch();
            List<MetaScalarRecord<Object>> scalars = dsl
                    .selectFrom(scalarTable)
                    .where(scalarTable.DATABASE.eq(database))
                    .fetch();
	        for (Table<?> table : schemaValidator.getExistingTables()) {
	        	if (!docParts.containsKey(table.getName())) {
	        		throw new InvalidDatabaseSchemaException(schemaName, "Table "+schemaName+"."+table.getName()
	        		+" has no container associated for database "+database);
	        	}
	        	
	        	MetaDocPartRecord<Object> docPart = docParts.get(table.getName());
	        	for (Field<?> existingField : table.fields()) {
	        		if (!sqlInterface.isAllowedColumnIdentifier(existingField.getName())) {
	        			continue;
	        		}
	        		if (!SchemaValidator.containsField(existingField, docPart.getCollection(), 
	        				docPart.getTableRefValue(tableRefFactory), fields, scalars, tableRefFactory)) {
	        			throw new InvalidDatabaseSchemaException(schemaName, "Column "+schemaName+"."+table.getName()
	        			+"."+existingField.getName()+" has no field associated for database "+database);
	        		}
	        	}
	        }
        }
        return metaSnapshotBuilder.build();
    }
    
	private Map<String,Map<String,Map<TableRef,Integer>>> loadRowIds(DSLContext dsl, MetaSnapshot snapshot) {
		Map<String,Map<String,Map<TableRef,Integer>>> megaMap = new HashMap<>();
		snapshot.streamMetaDatabases().forEach(db -> {
			Map<String,Map<TableRef,Integer>> collMap = new HashMap<>();
			megaMap.put(db.getName(), collMap);
			db.streamMetaCollections().forEach(collection -> {
				Map<TableRef,Integer> tableRefMap = new HashMap<>();
				collMap.put(collection.getName(), tableRefMap);
				collection.streamContainedMetaDocParts().forEach(metaDocPart -> {
					TableRef tableRef = metaDocPart.getTableRef();
					Integer lastRowIUsed = sqlInterface.getLastRowIdUsed(dsl, db, collection, metaDocPart);
					tableRefMap.put(tableRef, lastRowIUsed);
				});
			});
		});
		return megaMap;
	}
}
