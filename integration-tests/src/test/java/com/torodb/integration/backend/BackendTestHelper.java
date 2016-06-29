package com.torodb.integration.backend;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.jooq.DSLContext;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;
import com.torodb.backend.SqlInterface;
import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.d2r.RidGenerator;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaScalar;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import com.torodb.core.transaction.metainf.WrapperMutableMetaDocPart;
import com.torodb.d2r.CollectionMetaInfo;
import com.torodb.d2r.MockRidGenerator;
import com.torodb.d2r.model.DocPartDataImpl;
import com.torodb.d2r.model.DocPartRowImpl;
import com.torodb.d2r.model.TableMetadata;
import com.torodb.kvdocument.values.KVValue;

public class BackendTestHelper {

	private TestSchema schema;
	private SqlInterface sqlInterface;
	private RidGenerator ridGenerator = new MockRidGenerator();
	
	public BackendTestHelper(SqlInterface sqlInterface, TestSchema schema){
		this.sqlInterface = sqlInterface;
		this.schema = schema;
	}
	
	public void createMetaModel(DSLContext dsl) {
		String databaseName = schema.database.getName();
		String databaseSchemaName = schema.database.getIdentifier();
		String collectionName = schema.collection.getName();
		
		sqlInterface.getMetaDataWriteInterface().addMetaDatabase(dsl, databaseName, databaseSchemaName);
		sqlInterface.getStructureInterface().createSchema(dsl, databaseSchemaName);
		sqlInterface.getMetaDataWriteInterface().addMetaCollection(dsl, databaseName, collectionName, schema.collection.getIdentifier());
		sqlInterface.getMetaDataWriteInterface().addMetaDocPart(dsl, databaseName, collectionName, schema.rootDocPart.getTableRef(), schema.rootDocPart.getIdentifier());
		sqlInterface.getMetaDataWriteInterface().addMetaDocPart(dsl, databaseName, collectionName, schema.subDocPart.getTableRef(), schema.subDocPart.getIdentifier());
	}
    
    public void insertMetaFields(DSLContext dsl, MetaDocPart metaDocPart){
        metaDocPart.streamFields().forEach( metaField ->
            sqlInterface.getMetaDataWriteInterface().addMetaField(dsl, schema.database.getName(), schema.collection.getName(), metaDocPart.getTableRef(), 
                    metaField.getName(), metaField.getIdentifier(), metaField.getType())
        );
    }
    
    public void insertNewMetaFields(DSLContext dsl, MutableMetaDocPart metaDocPart){
        for (MetaField metaField : metaDocPart.getAddedMetaFields()) {
            sqlInterface.getMetaDataWriteInterface().addMetaField(dsl, schema.database.getName(), schema.collection.getName(), metaDocPart.getTableRef(), 
                    metaField.getName(), metaField.getIdentifier(), metaField.getType());
        }
    }
    
    public void createDocPartTable(DSLContext dsl, MetaCollection metaCollection, MetaDocPart metaDocPart) {
        if (metaDocPart.getTableRef().isRoot()) {
            sqlInterface.getStructureInterface().createRootDocPartTable(dsl, schema.database.getIdentifier(), metaDocPart.getIdentifier(), metaDocPart.getTableRef());
        } else {
            sqlInterface.getStructureInterface().createDocPartTable(dsl, schema.database.getIdentifier(), metaDocPart.getIdentifier(), metaDocPart.getTableRef(),
                    metaCollection.getMetaDocPartByTableRef(metaDocPart.getTableRef().getParent().get()).getIdentifier());
        }
        
        addColumnToDocPartTable(dsl, metaCollection, metaDocPart);
    }
    
    public void addColumnToDocPartTable(DSLContext dsl, MetaCollection metaCollection, MetaDocPart metaDocPart) {
        metaDocPart.streamScalars().forEach(metaScalar -> 
            sqlInterface.getStructureInterface().addColumnToDocPartTable(dsl, schema.database.getIdentifier(), metaDocPart.getIdentifier(), 
                    metaScalar.getIdentifier(), (DataTypeForKV<?>) sqlInterface.getDataTypeProvider().getDataType(metaScalar.getType()))
        );
        
        metaDocPart.streamFields().forEach(metaField -> 
            sqlInterface.getStructureInterface().addColumnToDocPartTable(dsl, schema.database.getIdentifier(), metaDocPart.getIdentifier(), 
                    metaField.getIdentifier(), (DataTypeForKV<?>) sqlInterface.getDataTypeProvider().getDataType(metaField.getType()))
        );
    }
    
    public void addNewColumnToDocPartTable(DSLContext dsl, MetaCollection metaCollection, MutableMetaDocPart mutableMetaDocPart) {
        for (MetaScalar metaScalar : mutableMetaDocPart.getAddedMetaScalars()) { 
            sqlInterface.getStructureInterface().addColumnToDocPartTable(dsl, schema.database.getIdentifier(), mutableMetaDocPart.getIdentifier(), 
                    metaScalar.getIdentifier(), (DataTypeForKV<?>) sqlInterface.getDataTypeProvider().getDataType(metaScalar.getType()));
        }
        
        for (MetaField metaField : mutableMetaDocPart.getAddedMetaFields()) { 
            sqlInterface.getStructureInterface().addColumnToDocPartTable(dsl, schema.database.getIdentifier(), mutableMetaDocPart.getIdentifier(), 
                    metaField.getIdentifier(), (DataTypeForKV<?>) sqlInterface.getDataTypeProvider().getDataType(metaField.getType()));
        }
    }
	
	public void insertDocPartData(DSLContext dsl, ImmutableMetaDocPart metaDocPart, 
								  List<ImmutableMap<String, Optional<KVValue<?>>>> values
			) {
	    IdentifierFactory factory = Mockito.mock(IdentifierFactory.class);
	    Mockito.when(factory.toFieldIdentifier(Mockito.any(), Mockito.any(), Mockito.eq("subDocPart"))).thenReturn("subDocPartField");
		MetaDatabase db = Mockito.mock(MetaDatabase.class);
		Mockito.when(db.getName()).thenReturn(schema.database.getName());
		Mockito.when(db.getIdentifier()).thenReturn(schema.database.getIdentifier());
		MutableMetaCollection col = Mockito.mock(MutableMetaCollection.class);
		Mockito.when(col.getName()).thenReturn(schema.collection.getName());
		Mockito.when(col.getIdentifier()).thenReturn(schema.collection.getIdentifier());

		CollectionMetaInfo metaInfo=new CollectionMetaInfo(db, col, factory, ridGenerator);
		Mockito.when(col.getMetaDocPartByTableRef(metaDocPart.getTableRef())).thenReturn(new WrapperMutableMetaDocPart(metaDocPart, t -> {}));
		
		TableMetadata tableMeta = new TableMetadata(metaInfo, metaDocPart.getTableRef());
		
		DocPartDataImpl docPartData = new DocPartDataImpl(tableMeta,null);
		int index = 0;
		for (Map<String, Optional<KVValue<?>>> docPartValueMap : values) {
		    DocPartRowImpl row = docPartData.newRowObject(index++, null);
		    
		    for (Map.Entry<String, Optional<KVValue<?>>> docPartValue : docPartValueMap.entrySet()) {
		        if (docPartValue.getValue().isPresent()) {
		            String key = docPartValue.getKey();
		            KVValue<?> kvValue = docPartValue.getValue().get();
                    row.addScalar(key, kvValue);
		        }
		    }
		}
		sqlInterface.getWriteInterface().insertDocPartData(dsl, schema.database.getIdentifier(), docPartData);
	}
	
}
