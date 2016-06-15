package com.torodb.backend;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import org.jooq.DSLContext;
import org.jooq.Field;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.core.TableRef;
import com.torodb.core.d2r.RidGenerator;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.WrapperMutableMetaDocPart;
import com.torodb.d2r.MockRidGenerator;
import com.torodb.kvdocument.values.KVValue;

public class BackendTestHelper {

	private DSLContext dsl;
	private TestSchema schema;
	private DatabaseInterface databaseInterface;
	
	public BackendTestHelper(DatabaseInterface databaseInterface, DSLContext dsl, TestSchema schema){
		this.databaseInterface = databaseInterface;
		this.dsl = dsl;
		this.schema = schema;
	}
	
	public void createMetaModel() {
		String databaseName = schema.databaseName;
		String databaseSchemaName = schema.databaseSchemaName;
		String collectionName = schema.collectionName;
		
		dsl.insertInto(databaseInterface.getMetaDatabaseTable())
		    .set(databaseInterface.getMetaDatabaseTable().newRecord().values(databaseName, databaseSchemaName))
		    .execute();
		dsl.execute(databaseInterface.createSchemaStatement(databaseSchemaName));
		dsl.insertInto(databaseInterface.getMetaCollectionTable())
		    .set(databaseInterface.getMetaCollectionTable().newRecord().values(databaseName, collectionName, schema.collectionIdentifierName))
		    .execute();
		dsl.insertInto(databaseInterface.getMetaDocPartTable())
		    .set(databaseInterface.getMetaDocPartTable().newRecord().values(databaseName, collectionName, schema.rootDocPartTableRef, schema.rootDocPartTableName))
		    .execute();
		dsl.insertInto(databaseInterface.getMetaDocPartTable())
         	.set(databaseInterface.getMetaDocPartTable().newRecord().values(databaseName, collectionName, schema.subDocPartTableRef, schema.subDocPartTableName))
         	.execute();		
	}
	
	public void insertMetaFields(TableRef tableRef, ImmutableMap<String, Field<?>> fields){
        for (Map.Entry<String, Field<?>> field : fields.entrySet()) {
            dsl.insertInto(databaseInterface.getMetaFieldTable())
                .set(databaseInterface.getMetaFieldTable().newRecord().values(schema.databaseName, schema.collectionName, tableRef, 
                        field.getKey(), field.getValue().getName(), 
                        FieldType.from(((DataTypeForKV<?>) field.getValue().getDataType()).getKVValueConverter().getErasuredType())))
                .execute();
        }
	}
	
	public void createDocPartTable(String tableName, Collection<? extends Field<?>> headerFields, Collection<Field<?>> fields){
		ArrayList<Field<?>> toAdd = new ArrayList<>();
		toAdd.addAll(headerFields);
		toAdd.addAll(fields);
		dsl.execute(databaseInterface.createDocPartTableStatement(dsl.configuration(), schema.databaseSchemaName, tableName, toAdd));
	}
	
	public void insertDocPartData(ImmutableMetaDocPart rootMetaDocPart, 
								  ImmutableList<ImmutableMap<String, Optional<KVValue<?>>>> values,
								  ImmutableMap<String, Field<?>> rootDocPartFields
			) {
		RidGenerator ridGenerator = new MockRidGenerator();
		DocPartDataImpl docPartData = new DocPartDataImpl(new WrapperMutableMetaDocPart(rootMetaDocPart, w -> {}), 
				ridGenerator.getDocPartRidGenerator(schema.databaseName, schema.collectionName));
		for (Map<String, Optional<KVValue<?>>> rootDocPartValueMap : values) {
		    DocPartRowImpl row = docPartData.appendRootRow();
		    for (Map.Entry<String, Optional<KVValue<?>>> rootDocPartValue : rootDocPartValueMap.entrySet()) {
		        if (rootDocPartValue.getValue().isPresent()) {
		            String key = rootDocPartValue.getKey();
					Field<?> field = rootDocPartFields.get(key);
		            docPartData.appendColumnValue(row, key, field.getName(), 
		                FieldType.from(((DataTypeForKV<?>) field.getDataType()).getKVValueConverter().getErasuredType()), 
		                rootDocPartValue.getValue().get());
		        }
		    }
		}
		databaseInterface.insertDocPartData(dsl, schema.databaseSchemaName, docPartData);
	}
}
