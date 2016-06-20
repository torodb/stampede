package com.torodb.backend;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.jooq.DSLContext;
import org.jooq.Field;

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
	private SqlInterface databaseInterface;
	private RidGenerator ridGenerator = new MockRidGenerator();
	
	public BackendTestHelper(SqlInterface databaseInterface, DSLContext dsl, TestSchema schema){
		this.databaseInterface = databaseInterface;
		this.dsl = dsl;
		this.schema = schema;
	}
	
	public void createMetaModel() {
		String databaseName = schema.databaseName;
		String databaseSchemaName = schema.databaseSchemaName;
		String collectionName = schema.collectionName;
		
		databaseInterface.addMetaDatabase(dsl, databaseName, databaseSchemaName);
		databaseInterface.createSchema(dsl, schema.databaseSchemaName);
		databaseInterface.addMetaCollection(dsl, databaseName, collectionName, schema.collectionIdentifierName);
		databaseInterface.addMetaDocPart(dsl, databaseName, collectionName, schema.rootDocPartTableRef, schema.rootDocPartTableName);
		databaseInterface.addMetaDocPart(dsl, databaseName, collectionName, schema.subDocPartTableRef, schema.subDocPartTableName);
	}
	
	public void insertMetaFields(TableRef tableRef, Map<String, Field<?>> fields){
		fields.forEach((key,value)->
			databaseInterface.addMetaField(dsl, schema.databaseName, schema.collectionName, tableRef, 
                    key, value.getName(), getType(value))
		);
	}
	
	public void createDocPartTable(String tableName, Collection<? extends Field<?>> headerFields, Collection<Field<?>> fields){
		ArrayList<Field<?>> toAdd = new ArrayList<>(headerFields);
		toAdd.addAll(fields);
		databaseInterface.createDocPartTable(dsl, schema.databaseSchemaName, tableName, toAdd);
	}
	
	public void insertDocPartData(ImmutableMetaDocPart metaDocPart, 
								  List<ImmutableMap<String, Optional<KVValue<?>>>> values,
								  Map<String, Field<?>> docPartFields
			) {
		DocPartDataImpl docPartData = new DocPartDataImpl(new WrapperMutableMetaDocPart(metaDocPart, w -> {}), 
				ridGenerator.getDocPartRidGenerator(schema.databaseName, schema.collectionName));
		for (Map<String, Optional<KVValue<?>>> docPartValueMap : values) {
		    DocPartRowImpl row = docPartData.appendRootRow();
		    for (Map.Entry<String, Optional<KVValue<?>>> docPartValue : docPartValueMap.entrySet()) {
		        if (docPartValue.getValue().isPresent()) {
		            String key = docPartValue.getKey();
					Field<?> field = docPartFields.get(key);
		            docPartData.appendColumnValue(row, key, field.getName(), 
		                getType(field),docPartValue.getValue().get());
		        }
		    }
		}
		databaseInterface.insertDocPartData(dsl, schema.databaseSchemaName, docPartData);
	}
	
	private FieldType getType(Field<?> field){
		return FieldType.from(((DataTypeForKV<?>) field.getDataType()).getKVValueConverter().getErasuredType());
	}
	
}
