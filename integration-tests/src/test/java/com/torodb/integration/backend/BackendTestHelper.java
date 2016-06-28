package com.torodb.integration.backend;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;
import com.torodb.backend.SqlInterface;
import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.core.TableRef;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.d2r.RidGenerator;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.WrapperMutableMetaDocPart;
import com.torodb.d2r.CollectionMetaInfo;
import com.torodb.d2r.MockRidGenerator;
import com.torodb.d2r.model.DocPartDataImpl;
import com.torodb.d2r.model.DocPartRowImpl;
import com.torodb.d2r.model.TableMetadata;
import com.torodb.kvdocument.values.KVValue;

public class BackendTestHelper {

	private DSLContext dsl;
	private TestSchema schema;
	private SqlInterface sqlInterface;
	private RidGenerator ridGenerator = new MockRidGenerator();
	private IdentifierFactory factory = Mockito.mock(IdentifierFactory.class);
	
	public BackendTestHelper(SqlInterface sqlInterface, DSLContext dsl, TestSchema schema){
		this.sqlInterface = sqlInterface;
		this.dsl = dsl;
		this.schema = schema;
	}
	
	public void createMetaModel() {
		String databaseName = schema.databaseName;
		String databaseSchemaName = schema.databaseSchemaName;
		String collectionName = schema.collectionName;
		
		sqlInterface.addMetaDatabase(dsl, databaseName, databaseSchemaName);
		sqlInterface.createSchema(dsl, schema.databaseSchemaName);
		sqlInterface.addMetaCollection(dsl, databaseName, collectionName, schema.collectionIdentifierName);
		sqlInterface.addMetaDocPart(dsl, databaseName, collectionName, schema.rootDocPartTableRef, schema.rootDocPartTableName);
		sqlInterface.addMetaDocPart(dsl, databaseName, collectionName, schema.subDocPartTableRef, schema.subDocPartTableName);
	}
	
	public void insertMetaFields(TableRef tableRef, Map<String, Field<?>> fields){
		fields.forEach((key,value)->
			sqlInterface.addMetaField(dsl, schema.databaseName, schema.collectionName, tableRef, 
                    key, value.getName(), getType(value))
		);
	}
	
	public void createDocPartTable(String tableName, Collection<? extends Field<?>> headerFields, Collection<Field<?>> fields){
		ArrayList<Field<?>> toAdd = new ArrayList<>(headerFields);
		toAdd.addAll(fields);
		sqlInterface.createDocPartTable(dsl, schema.databaseSchemaName, tableName, toAdd);
	}
	
	public void insertDocPartData(ImmutableMetaDocPart metaDocPart, 
								  List<ImmutableMap<String, Optional<KVValue<?>>>> values,
								  Map<String, Field<?>> docPartFields
			) {
		MetaDatabase db = Mockito.mock(MetaDatabase.class);
		Mockito.when(db.getName()).thenReturn(schema.databaseName);
		Mockito.when(db.getIdentifier()).thenReturn(schema.databaseSchemaName);
		MutableMetaCollection col = Mockito.mock(MutableMetaCollection.class);
		Mockito.when(col.getName()).thenReturn(schema.collectionName);
		Mockito.when(col.getIdentifier()).thenReturn(schema.collectionIdentifierName);

		CollectionMetaInfo metaInfo=new CollectionMetaInfo(db, col, factory, ridGenerator);
		Mockito.when(col.getMetaDocPartByTableRef(metaDocPart.getTableRef())).thenReturn(new WrapperMutableMetaDocPart(metaDocPart, new Consumer<WrapperMutableMetaDocPart>() {
			
			@Override
			public void accept(WrapperMutableMetaDocPart t) {
				// TODO Auto-generated method stub
				
			}
		}));
		
		TableMetadata tableMeta = new TableMetadata(metaInfo, metaDocPart.getTableRef());
		
		DocPartDataImpl docPartData = new DocPartDataImpl(tableMeta,null);
		for (Map<String, Optional<KVValue<?>>> docPartValueMap : values) {
		    DocPartRowImpl row = docPartData.newRowObject(0,null);
		    
		    for (Map.Entry<String, Optional<KVValue<?>>> docPartValue : docPartValueMap.entrySet()) {
		        if (docPartValue.getValue().isPresent()) {
		            String key = docPartValue.getKey();
		            KVValue<?> kvValue = docPartValue.getValue().get();
					row.addScalar(key,kvValue);
		        }
		    }
		}
		sqlInterface.insertDocPartData(dsl, schema.databaseSchemaName, docPartData);
	}
	
	private FieldType getType(Field<?> field){
		return FieldType.from(((DataTypeForKV<?>) field.getDataType()).getKVValueConverter().getErasuredType());
	}
	
}
