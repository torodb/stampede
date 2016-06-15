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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.sql.DataSource;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.Assert;
import org.junit.Test;

import com.torodb.backend.AbstractBackendTest;
import com.torodb.backend.BackendDocumentTestHelper;
import com.torodb.backend.BackendTestHelper;
import com.torodb.backend.DatabaseInterface;
import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.backend.exceptions.InvalidDatabaseException;
import com.torodb.backend.meta.TorodbMeta;
import com.torodb.core.d2r.CollectionData;
import com.torodb.core.d2r.DocPartResults;
import com.torodb.core.document.ToroDocument;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.ImmutableMetaField;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaSnapshot;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import com.torodb.core.transaction.metainf.WrapperMutableMetaSnapshot;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVInteger;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.kvdocument.values.heap.ListKVArray;

public class BackendDerbyTest extends AbstractBackendTest {
	
    @Test
    public void testTorodbMeta() throws Exception {
        try (Connection connection = dataSource.getConnection()) {
            TorodbMeta tododbMeta = buildTorodbMeta(connection);
            connection.commit();
            assertFalse(tododbMeta.getCurrentMetaSnapshot().streamMetaDatabases().iterator().hasNext());
        }
        
        try (Connection connection = dataSource.getConnection()) {
        	TorodbMeta tododbMeta = buildTorodbMeta(connection);
            connection.commit();
            assertFalse(tododbMeta.getCurrentMetaSnapshot().streamMetaDatabases().iterator().hasNext());
        }
    }
    
    @Test
    public void testTorodbMetaStoreAndReload() throws Exception {
        try (Connection connection = dataSource.getConnection()) {
        	buildTorodbMeta(connection);
            connection.commit();
        }
        try (Connection connection = dataSource.getConnection()) {
            BackendTestHelper helper = new BackendTestHelper(databaseInterface, dsl(connection), schema);
            helper.createMetaModel();
            helper.insertMetaFields(schema.rootDocPartTableRef, schema.rootDocPartFields);
            helper.createDocPartTable(schema.rootDocPartTableName, databaseInterface.getMetaDocPartTable().ROOT_FIELDS, schema.rootDocPartFields.values());
            helper.insertMetaFields(schema.subDocPartTableRef, schema.subDocPartFields);
            helper.createDocPartTable(schema.subDocPartTableName, databaseInterface.getMetaDocPartTable().FIRST_FIELDS, schema.subDocPartFields.values());
            connection.commit();
        }
        TorodbMeta torodbMeta;
        try (Connection connection = dataSource.getConnection()) {
            torodbMeta = buildTorodbMeta(connection);
            connection.commit();
        }
        
        MetaSnapshot metaSnapshot = torodbMeta.getCurrentMetaSnapshot();
        MetaDatabase metaDatabase = metaSnapshot.getMetaDatabaseByName(schema.databaseName);
        
        assertNotNull(metaDatabase);
        assertEquals(schema.databaseSchemaName, metaDatabase.getIdentifier());
        
        MetaCollection metaCollection = metaDatabase.getMetaCollectionByName(schema.collectionName);
        assertNotNull(metaCollection);
        assertEquals(schema.collectionIdentifierName, metaCollection.getIdentifier());
        
        MetaDocPart rootMetaDocPart = metaCollection.getMetaDocPartByTableRef(schema.rootDocPartTableRef);
        assertNotNull(rootMetaDocPart);
        assertEquals(schema.rootDocPartTableName, rootMetaDocPart.getIdentifier());
        assertAllFieldsArePresent(schema.rootDocPartFields,rootMetaDocPart);
        
        final MetaDocPart subDocMetaDocPart = metaCollection.getMetaDocPartByTableRef(schema.subDocPartTableRef);
        assertNotNull(subDocMetaDocPart);
        assertEquals(schema.subDocPartTableName, subDocMetaDocPart.getIdentifier());
        assertAllFieldsArePresent(schema.subDocPartFields,subDocMetaDocPart);
        
        try (Connection connection = dataSource.getConnection()) {
        	DSLContext dsl = dsl(connection);
        	BackendTestHelper helper = new BackendTestHelper(databaseInterface, dsl, schema);
            helper.insertMetaFields(schema.subDocPartTableRef, schema.newSubDocPartFields);
            for (Field<?> field: schema.newSubDocPartFields.values()){
            	dsl.execute(databaseInterface.addColumnToDocPartTableStatement(dsl.configuration(), schema.databaseSchemaName, schema.subDocPartTableName, field));
            }
            connection.commit();
        }
        
        try (Connection connection = dataSource.getConnection()) {
            torodbMeta = buildTorodbMeta(connection);
            connection.commit();
        }
        
        metaSnapshot = torodbMeta.getCurrentMetaSnapshot();
        metaDatabase = metaSnapshot.getMetaDatabaseByName(schema.databaseName);
        
        assertNotNull(metaDatabase);
        assertEquals(schema.databaseSchemaName, metaDatabase.getIdentifier());
        
        metaCollection = metaDatabase.getMetaCollectionByName(schema.collectionName);
        assertNotNull(metaCollection);
        assertEquals(schema.collectionIdentifierName, metaCollection.getIdentifier());
        
        rootMetaDocPart = metaCollection.getMetaDocPartByTableRef(schema.rootDocPartTableRef);
        assertNotNull(rootMetaDocPart);
        assertEquals(schema.rootDocPartTableName, rootMetaDocPart.getIdentifier());
        assertAllFieldsArePresent(schema.rootDocPartFields, rootMetaDocPart);
        
        MetaDocPart newSubDocMetaDocPart = metaCollection.getMetaDocPartByTableRef(schema.subDocPartTableRef);
        assertNotNull(newSubDocMetaDocPart);
        assertEquals(schema.subDocPartTableName, newSubDocMetaDocPart.getIdentifier());
        assertAllFieldsArePresent(schema.subDocPartFields, newSubDocMetaDocPart);
        assertAllFieldsArePresent(schema.newSubDocPartFields, newSubDocMetaDocPart);
    }

	private void assertAllFieldsArePresent(Map<String, Field<?>> docPartFields, MetaDocPart rootMetaDocPart) {
		docPartFields.forEach( (key, field) -> {
            MetaField metaField = rootMetaDocPart.getMetaFieldByNameAndType(key, fieldType(field));
			assertNotNull(metaField);
            assertEquals(field.getName(), metaField.getIdentifier());
        });
	}

	@Test
    public void testConsumeRids() throws Exception {
    	 try (Connection connection = dataSource.getConnection()) {
    		 buildTorodbMeta(connection);
             connection.commit();
         }
         try (Connection connection = dataSource.getConnection()) {
             DSLContext dsl = dsl(connection);
             BackendTestHelper helper = new BackendTestHelper(databaseInterface, dsl, schema);
             helper.createMetaModel();
             int first100RootRid = databaseInterface.consumeRids(dsl, schema.databaseName, schema.collectionName, schema.rootDocPartTableRef, 100);
             assertEquals(0, first100RootRid);
             int next100RootRid = databaseInterface.consumeRids(dsl, schema.databaseName, schema.collectionName, schema.rootDocPartTableRef, 100);
             assertEquals(100, next100RootRid);
             helper.createDocPartTable(schema.rootDocPartTableName, databaseInterface.getMetaDocPartTable().ROOT_FIELDS, schema.rootDocPartFields.values());
             int first100SubRid = databaseInterface.consumeRids(dsl, schema.databaseName, schema.collectionName, schema.subDocPartTableRef, 100);
             assertEquals(0, first100SubRid);
             int next100SubRid = databaseInterface.consumeRids(dsl, schema.databaseName, schema.collectionName, schema.subDocPartTableRef, 100);
             assertEquals(100, next100SubRid);
             connection.commit();
         }
    }
    

    @Test
    public void testTorodbLastRowId() throws Exception {
    	 try (Connection connection = dataSource.getConnection()) {
    		 buildTorodbMeta(connection);
             connection.commit();
         }
    	 
        try (Connection connection = dataSource.getConnection()) {
        	DSLContext dsl = dsl(connection);
        	BackendTestHelper helper = new BackendTestHelper(databaseInterface, dsl, schema);
        	helper.createMetaModel();
            helper.insertMetaFields(schema.rootDocPartTableRef, schema.rootDocPartFields);
            helper.createDocPartTable(schema.rootDocPartTableName,
            		databaseInterface.getMetaDocPartTable().ROOT_FIELDS, 
            		schema.rootDocPartFields.values());
            helper.insertMetaFields(schema.subDocPartTableRef, schema.subDocPartFields);
            helper.createDocPartTable(schema.subDocPartTableName, 
            		databaseInterface.getMetaDocPartTable().FIRST_FIELDS, 
            		schema.subDocPartFields.values());

            DerbyTorodbMeta torodbMeta = new DerbyTorodbMeta(dsl, tableRefFactory, databaseInterface);
            ImmutableMetaSnapshot snapshot = torodbMeta.getCurrentMetaSnapshot();
            
            ImmutableMetaDatabase metaDatabase = snapshot.getMetaDatabaseByName(schema.databaseName);
        	ImmutableMetaCollection metaCollection = metaDatabase.getMetaCollectionByName(schema.collectionName);
        	ImmutableMetaDocPart rootMetaDocPart = metaCollection.getMetaDocPartByTableRef(schema.rootDocPartTableRef);
        	int lastRootRowIUsed = databaseInterface.getLastRowIUsed(dsl, metaDatabase, metaCollection, rootMetaDocPart);
        	assertEquals(0, lastRootRowIUsed);
        	
        	helper.insertDocPartData(rootMetaDocPart, schema.rootDocPartValues, schema.rootDocPartFields);
        	
        	lastRootRowIUsed = databaseInterface.getLastRowIUsed(dsl, metaDatabase, metaCollection, rootMetaDocPart);
        	assertEquals(1, lastRootRowIUsed);
        }
    }
    
    @Test
    public void testTorodbInsertDocPart() throws Exception {
        try (Connection connection = dataSource.getConnection()) {
        	buildTorodbMeta(connection);
            connection.commit();
        }
        try (Connection connection = dataSource.getConnection()) {
            DSLContext dsl = dsl(connection);
            BackendTestHelper helper = new BackendTestHelper(databaseInterface, dsl, schema);
            
            dsl.execute(databaseInterface.createSchemaStatement(schema.databaseSchemaName));
            
            ImmutableMetaDocPart.Builder rootMetaDocPartBuilder = new ImmutableMetaDocPart.Builder(schema.rootDocPartTableRef, schema.rootDocPartTableName);
            schema.rootDocPartFields.forEach( (key, field) ->{
            	rootMetaDocPartBuilder.add(new ImmutableMetaField(key, field.getName(), fieldType(field)));
            });
            
            ImmutableMetaDocPart rootMetaDocPart = rootMetaDocPartBuilder.build();
            helper.createDocPartTable(schema.rootDocPartTableName, databaseInterface.getDocPartTableInternalFields(rootMetaDocPart), schema.rootDocPartFields.values());
            helper.insertDocPartData(rootMetaDocPart, schema.rootDocPartValues, schema.rootDocPartFields);
            connection.commit();
            
            StringBuilder rootDocPartSelectStatementBuilder = new StringBuilder("SELECT ");
            for (Map.Entry<String, Field<?>> field : schema.rootDocPartFields.entrySet()) {
                rootDocPartSelectStatementBuilder.append('"')
                    .append(field.getValue().getName())
                    .append("\",");
            }
            rootDocPartSelectStatementBuilder.setCharAt(rootDocPartSelectStatementBuilder.length() - 1, ' ');
            rootDocPartSelectStatementBuilder.append("FROM \"")
                    .append(schema.databaseSchemaName)
                    .append("\".\"")
                    .append(schema.rootDocPartTableName)
                    .append('"');
            try (PreparedStatement preparedStatement = connection.prepareStatement(rootDocPartSelectStatementBuilder.toString())) {
                ResultSet resultSet = preparedStatement.executeQuery();
                List<Integer> foundRowIndexes = new ArrayList<>();
                while (resultSet.next()) {
                    boolean rowFound = findRow(resultSet, foundRowIndexes);
                    if (!rowFound) {
                        StringBuilder resultSetRowBuilder = new StringBuilder();
                        final int columnCount = resultSet.getMetaData().getColumnCount();
                        for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
                            resultSetRowBuilder.append(resultSet.getObject(columnIndex))
                                .append(", ");
                        }
                        Assert.fail("Row " + resultSetRowBuilder.toString() + " not found");
                    }
                }
            }
        }
    }

	private boolean findRow(ResultSet resultSet, List<Integer> foundRowIndexes) throws SQLException {
		Integer index = 0;
		boolean rowFound = true;
		for (Map<String, Optional<KVValue<?>>> rootDocPartValueMap : schema.rootDocPartValues) {
		    rowFound = true;
		    int columnIndex = 1;
		    for (Map.Entry<String, Field<?>> field : schema.rootDocPartFields.entrySet()) {
		        Optional<KVValue<?>> value = rootDocPartValueMap.get(field.getKey());
		        DataTypeForKV<?> dataTypeForKV = (DataTypeForKV<?>) field.getValue().getDataType();
		        Object databaseValue = resultSet.getObject(columnIndex);
		        Optional<KVValue<?>> databaseConvertedValue;
		        if (resultSet.wasNull()) {
		            databaseConvertedValue = Optional.empty();
		        } else {
		            databaseConvertedValue = Optional.of(dataTypeForKV.convert(databaseValue));
		        }
		        columnIndex++;
		        
		        if (!value.isPresent()) {
		            if (databaseConvertedValue.isPresent()) {
		                rowFound = false;
		                break;
		            }
		        } else if (!databaseConvertedValue.isPresent()) {
		            rowFound = false;
		            break;
		        } else if (!value.get().equals(databaseConvertedValue.get())) {
		            rowFound = false;
		            break;
		        }
		    }
		    
		    if (rowFound && !foundRowIndexes.contains(index)) {
		        foundRowIndexes.add(index);
		        break;
		    }
		    
		    index++;
		}
		return rowFound;
	}
    

    @Test
    public void testTorodbReadCollectionResultSets() throws Exception {
    	BackendDocumentTestHelper helper = new BackendDocumentTestHelper(databaseInterface, tableRefFactory, schema);
        KVDocument document = helper.parseFromJson("testTorodbReadDocPart.json");
        try (Connection connection = dataSource.getConnection()) {
        	DSLContext dsl = dsl(connection);
        	MutableMetaSnapshot mutableSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot.Builder().build());
        	mutableSnapshot
        		.addMetaDatabase(schema.databaseName, schema.databaseSchemaName)
        		.addMetaCollection(schema.collectionName, schema.collectionIdentifierName);
            dsl.execute(databaseInterface.createSchemaStatement(schema.databaseSchemaName));
            CollectionData collectionData = helper.parseDocumentAndCreateDocPartDataTables(mutableSnapshot, dsl, document);
            
            List<Integer> generatedDids = helper.writeCollectionData(dsl, collectionData);
            
            MetaDatabase metaDatabase = mutableSnapshot.getMetaDatabaseByName(schema.databaseName);
            MetaCollection metaCollection = metaDatabase.getMetaCollectionByName(schema.collectionName);
            
            DocPartResults<ResultSet> docPartResultSets = databaseInterface.getCollectionResultSets(
                    dsl, metaDatabase, metaCollection, 
                    generatedDids);
            
            Collection<ToroDocument> readedDocuments = helper.readDocuments(metaDatabase, metaCollection, docPartResultSets);
            
            KVDocument readedDocument = readedDocuments.iterator().next().getRoot();
            System.out.println(document);
            System.out.println(readedDocument);
            assertEquals(document, readedDocument);
        }
    }
    
    @Test
    public void testTorodbReadCollectionResultSetsWithStructures() throws Exception {
        try (Connection connection = dataSource.getConnection()) {
            DSLContext dsl = dsl(connection);
            MutableMetaSnapshot mutableSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot.Builder().build());
            mutableSnapshot
            	.addMetaDatabase(schema.databaseName, schema.databaseSchemaName)
            	.addMetaCollection(schema.collectionName, schema.collectionIdentifierName);
            dsl.execute(databaseInterface.createSchemaStatement(schema.databaseSchemaName));
            
            List<KVDocument> documents = new ArrayList<>();
            for (int current_size = 0; current_size < 5; current_size++) {
                int[] array_scalars_values = new int[] { 0 };
                if (current_size > 0) {
                    array_scalars_values = new int[] { 0, 1, 2 };
                }
                for (int array_scalars : array_scalars_values) {
                    for (int object_size=0; object_size < 4; object_size++) {
                        int[] object_index_values=new int[] { 0 };
                        if (object_size > 0 && current_size > 0) {
                            object_index_values=new int[0];
                            for (int i=1; i<current_size; i++) {
                                object_index_values=Arrays.copyOf(object_index_values, object_index_values.length + 1);
                                object_index_values[object_index_values.length - 1] = i;
                            }
                        }
                        for (int object_index : object_index_values) {
                            KVDocument.Builder documentBuilder = new KVDocument.Builder();
                            int current_index = 0;
                            if (object_index == current_index) {
                               if (object_size == 1)
                                   documentBuilder.putValue("k", new KVDocument.Builder().build());
                               else if (object_size == 2)
                                   documentBuilder.putValue("k", new KVDocument.Builder()
                                           .putValue("k", KVInteger.of(1))
                                           .build());
                               else if (object_size != 0)
                                   continue;
                            }
                            if (current_size > 0) {
                                current_index = current_index + 1;
                                List<KVValue<?>> array_value = new ArrayList<>();
                                if (object_index == current_index) {
                                    if (object_size == 1) 
                                       array_value.add(new KVDocument.Builder().build());
                                    else if (object_size == 2)
                                       array_value.add(new KVDocument.Builder()
                                               .putValue("k", KVInteger.of(1))
                                               .build());
                                    else if (object_size != 0)
                                       continue;
                                }
                                if (array_scalars > 0) {
                                    if (array_scalars == 1)
                                       array_value.add(new ListKVArray(Arrays.asList(new KVValue<?>[] { KVInteger.of(1) })));
                                   else
                                       array_value.add(new ListKVArray(Arrays.asList(new KVValue<?>[] { KVInteger.of(1), KVInteger.of(2) })));
                                }
                                if (current_size > 1) {
                                    int[] size_values=new int[0];
                                    for (int i=current_size; i>=2; i--) {
                                        size_values=Arrays.copyOf(size_values, size_values.length + 1);
                                        size_values[size_values.length - 1] = i;
                                    }
                                    for (@SuppressWarnings("unused") int size : size_values) {
                                        current_index = current_index + 1;
                                        array_value = new ArrayList<>(Arrays.asList(new KVValue<?>[] { new ListKVArray(array_value) }));
                                        if (object_index == current_index) {
                                            if (object_size == 1)
                                                array_value.add(new ListKVArray(Arrays.asList(new KVValue<?>[] { new KVDocument.Builder().build() })));
                                            else if (object_size == 2)
                                                array_value.add(new ListKVArray(Arrays.asList(new KVValue<?>[] { new KVDocument.Builder()
                                                    .putValue("k", KVInteger.of(1))
                                                    .build() })));
                                            else if (object_size != 0)
                                                array_value = new ArrayList<>(Arrays.asList(new KVValue<?>[] { new KVDocument.Builder()
                                                    .putValue("k", new ListKVArray(array_value))
                                                    .build() }));
                                        }
                                        if (array_scalars > 0) {
                                            if (array_scalars == 1)
                                               array_value.add(new ListKVArray(Arrays.asList(new KVValue<?>[] { KVInteger.of(1) })));
                                           else
                                               array_value.add(new ListKVArray(Arrays.asList(new KVValue<?>[] { KVInteger.of(1), KVInteger.of(2) })));
                                        }
                                    }
                                }
                                documentBuilder.putValue("k", new ListKVArray(array_value));;
                            }
                            documents.add(documentBuilder.build());
                        }
                    }
                }
            }
            
            BackendDocumentTestHelper helper = new BackendDocumentTestHelper(databaseInterface, tableRefFactory, schema);
            helper.parseDocumentsAndCreateDocPartDataTables(mutableSnapshot, dsl, documents);
            
            for (KVDocument document : documents) {
                CollectionData collectionData = helper.readDataFromDocuments(schema.databaseName, schema.collectionName, Arrays.asList(document), mutableSnapshot);
                
                List<Integer> generatedDids = helper.writeCollectionData(dsl, collectionData);
                
                MetaDatabase metaDatabase = mutableSnapshot.getMetaDatabaseByName(schema.databaseName);
                MetaCollection metaCollection = metaDatabase.getMetaCollectionByName(schema.collectionName);
                
                DocPartResults<ResultSet> docPartResultSets = databaseInterface.getCollectionResultSets(
                        dsl, metaDatabase, metaCollection, 
                        generatedDids);
                
                Collection<ToroDocument> readedDocuments = helper.readDocuments(metaDatabase, metaCollection, docPartResultSets);
                
                KVDocument readedDocument = readedDocuments.iterator().next().getRoot();
                System.out.println("Written :" + document);
                System.out.println("Readed: " + readedDocument);
                assertEquals(document, readedDocument);
            }
            CollectionData collectionData = helper.readDataFromDocuments(schema.databaseName, schema.collectionName, documents, mutableSnapshot);
            
            List<Integer> generatedDids = helper.writeCollectionData(dsl, collectionData);
            
            MetaDatabase metaDatabase = mutableSnapshot.getMetaDatabaseByName(schema.databaseName);
            MetaCollection metaCollection = metaDatabase.getMetaCollectionByName(schema.collectionName);
            
            DocPartResults<ResultSet> docPartResultSets = databaseInterface.getCollectionResultSets(
                    dsl, metaDatabase, metaCollection, 
                    generatedDids);
            
            Collection<ToroDocument> readedDocuments = helper.readDocuments(metaDatabase, metaCollection, docPartResultSets);
            System.out.println("Written :" + documents);
            System.out.println("Readed: " + readedDocuments);
            assertEquals(documents.size(), readedDocuments.size());
        }
    }

    @Override
    protected DatabaseInterface createDatabaseInterface() {
        return new DerbyDatabaseInterface(tableRefFactory);
    }

    @Override
    protected DataSource createDataSource()  {
    	return Derby.getDatasource();
    }
    
	@Override
	protected void cleanDatabase(DatabaseInterface databaseInterface, DataSource dataSource) throws SQLException {
		Derby.cleanDatabase(databaseInterface, dataSource);
	}

    private FieldType fieldType(Field<?> field) {
    	return FieldType.from(((DataTypeForKV<?>) field.getDataType()).getKVValueConverter().getErasuredType());
    }
    
    private TorodbMeta buildTorodbMeta(Connection connection) throws SQLException, IOException, InvalidDatabaseException{
    	return new DerbyTorodbMeta(dsl(connection), tableRefFactory, databaseInterface);
    }
    
	private DSLContext dsl(Connection connection){
		return DSL.using(connection, SQLDialect.DERBY);
	}
}