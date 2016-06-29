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

package com.torodb.integration.backend;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Converter;
import org.jooq.DSLContext;
import org.junit.Assert;
import org.junit.Test;

import com.torodb.backend.DefaultDidCursor;
import com.torodb.backend.MockDidCursor;
import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.backend.tables.MetaDocPartTable;
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
import com.torodb.kvdocument.types.DocumentType;
import com.torodb.kvdocument.values.KVBoolean;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVInteger;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.kvdocument.values.heap.ListKVArray;
import com.torodb.kvdocument.values.heap.StringKVString;

public class BackendIntegrationTest extends AbstractBackendTest {
    
    private final Logger LOGGER = LogManager.getLogger(BackendIntegrationTest.class);
    
    /**
     * This test whether the snapshot can be build and if it can be rebuild.
     * @throws Exception
     */
    @Test
    public void testSnapshotUpdater() throws Exception {
        MetaSnapshot metaSnapshot = buildMetaSnapshot();
        assertFalse(metaSnapshot.streamMetaDatabases().iterator().hasNext());
        
    	metaSnapshot = buildMetaSnapshot();
        assertFalse(metaSnapshot.streamMetaDatabases().iterator().hasNext());
    }
    
    @Test
    public void testSnapshotUpdaterStoreAndReload() throws Exception {
       	buildMetaSnapshot();

       	try (Connection connection = sqlInterface.getDbBackend().createWriteConnection()) {
       	    DSLContext dsl = sqlInterface.getDslContextFactory().createDSLContext(connection);
            createMetaModel(dsl);
            insertMetaFields(dsl, data.rootDocPart);
            createDocPartTable(dsl, data.collection, data.rootDocPart);
            insertMetaFields(dsl, data.subDocPart);
            createDocPartTable(dsl, data.collection, data.subDocPart);
            connection.commit();
        }
        
        MetaSnapshot metaSnapshot = buildMetaSnapshot();
        MetaDatabase metaDatabase = metaSnapshot.getMetaDatabaseByName(data.database.getName());
        
        assertNotNull(metaDatabase);
        assertEquals(data.database.getIdentifier(), metaDatabase.getIdentifier());
        
        MetaCollection metaCollection = metaDatabase.getMetaCollectionByName(data.collection.getName());
        assertNotNull(metaCollection);
        assertEquals(data.collection.getIdentifier(), metaCollection.getIdentifier());
        
        MetaDocPart rootMetaDocPart = metaCollection.getMetaDocPartByTableRef(data.rootDocPart.getTableRef());
        assertNotNull(rootMetaDocPart);
        assertEquals(data.rootDocPart.getIdentifier(), rootMetaDocPart.getIdentifier());
        assertAllFieldsArePresent(data.rootDocPart, rootMetaDocPart);
        
        final MetaDocPart subDocMetaDocPart = metaCollection.getMetaDocPartByTableRef(data.subDocPart.getTableRef());
        assertNotNull(subDocMetaDocPart);
        assertEquals(data.subDocPart.getIdentifier(), subDocMetaDocPart.getIdentifier());
        assertAllFieldsArePresent(data.subDocPart,subDocMetaDocPart);
        
        try (Connection connection = sqlInterface.getDbBackend().createWriteConnection()) {
        	DSLContext dsl = sqlInterface.getDslContextFactory().createDSLContext(connection);
            insertNewMetaFields(dsl, data.newSubDocPart);
            addNewColumnToDocPartTable(dsl, data.collection, data.newSubDocPart);
            connection.commit();
        }
        
        metaSnapshot = buildMetaSnapshot();
        metaDatabase = metaSnapshot.getMetaDatabaseByName(data.database.getName());
        
        assertNotNull(metaDatabase);
        assertEquals(data.database.getIdentifier(), metaDatabase.getIdentifier());
        
        metaCollection = metaDatabase.getMetaCollectionByName(data.collection.getName());
        assertNotNull(metaCollection);
        assertEquals(data.collection.getIdentifier(), metaCollection.getIdentifier());
        
        rootMetaDocPart = metaCollection.getMetaDocPartByTableRef(data.rootDocPart.getTableRef());
        assertNotNull(rootMetaDocPart);
        assertEquals(data.rootDocPart.getIdentifier(), rootMetaDocPart.getIdentifier());
        assertAllFieldsArePresent(data.rootDocPart, rootMetaDocPart);
        
        MetaDocPart newSubDocMetaDocPart = metaCollection.getMetaDocPartByTableRef(data.subDocPart.getTableRef());
        assertNotNull(newSubDocMetaDocPart);
        assertEquals(data.subDocPart.getIdentifier(), newSubDocMetaDocPart.getIdentifier());
        assertAllFieldsArePresent(data.subDocPart, newSubDocMetaDocPart);
        assertAllFieldsArePresent(data.newSubDocPart, newSubDocMetaDocPart);
    }

	private void assertAllFieldsArePresent(MetaDocPart docPart, MetaDocPart rootMetaDocPart) {
		docPart.streamFields().forEach( field -> {
            MetaField metaField = rootMetaDocPart.getMetaFieldByNameAndType(field.getName(), field.getType());
			assertNotNull(metaField);
            assertEquals(field.getName(), metaField.getName());
            assertEquals(field.getIdentifier(), metaField.getIdentifier());
            assertEquals(field.getType(), metaField.getType());
        });
	}

	@Test
    public void testConsumeRids() throws Exception {
		 buildMetaSnapshot();

		 try (Connection connection = sqlInterface.getDbBackend().createWriteConnection()) {
             DSLContext dsl = sqlInterface.getDslContextFactory().createDSLContext(connection);
             createMetaModel(dsl);
             int first100RootRid = sqlInterface.getMetaDataWriteInterface().consumeRids(dsl, data.database.getName(), data.collection.getName(), data.rootDocPart.getTableRef(), 100);
             assertEquals(0, first100RootRid);
             int next100RootRid = sqlInterface.getMetaDataWriteInterface().consumeRids(dsl, data.database.getName(), data.collection.getName(), data.rootDocPart.getTableRef(), 100);
             assertEquals(100, next100RootRid);
             createDocPartTable(dsl, data.collection, data.rootDocPart);
             int first100SubRid = sqlInterface.getMetaDataWriteInterface().consumeRids(dsl, data.database.getName(), data.collection.getName(), data.subDocPart.getTableRef(), 100);
             assertEquals(0, first100SubRid);
             int next100SubRid = sqlInterface.getMetaDataWriteInterface().consumeRids(dsl, data.database.getName(), data.collection.getName(), data.subDocPart.getTableRef(), 100);
             assertEquals(100, next100SubRid);
             connection.commit();
         }
    }

    @Test
    public void testTorodbLastRowId() throws Exception {
		buildMetaSnapshot();
    	 
        try (Connection connection = sqlInterface.getDbBackend().createWriteConnection()) {
        	DSLContext dsl = sqlInterface.getDslContextFactory().createDSLContext(connection);
        	createMetaModel(dsl);
            insertMetaFields(dsl, data.rootDocPart);
            createDocPartTable(dsl, data.collection,
            		data.rootDocPart);
            insertMetaFields(dsl, data.subDocPart);
            createDocPartTable(dsl, data.collection, 
            		data.subDocPart);
            connection.commit();

            ImmutableMetaSnapshot snapshot = buildMetaSnapshot();
            
            ImmutableMetaDatabase metaDatabase = snapshot.getMetaDatabaseByName(data.database.getName());
        	ImmutableMetaCollection metaCollection = metaDatabase.getMetaCollectionByName(data.collection.getName());
        	ImmutableMetaDocPart rootMetaDocPart = metaCollection.getMetaDocPartByTableRef(data.rootDocPart.getTableRef());
        	ImmutableMetaDocPart subDocMetaDocPart = metaCollection.getMetaDocPartByTableRef(data.subDocPart.getTableRef());
        	
        	int lastRootRowIUsed = sqlInterface.getReadInterface().getLastRowIdUsed(dsl, metaDatabase, metaCollection, rootMetaDocPart);
        	int lastSubDocRowIUsed = sqlInterface.getReadInterface().getLastRowIdUsed(dsl, metaDatabase, metaCollection, subDocMetaDocPart);
        	assertEquals(-1, lastRootRowIUsed);
        	assertEquals(-1, lastSubDocRowIUsed);
        	
            MutableMetaSnapshot mutableSnapshot = new WrapperMutableMetaSnapshot(data.snapshot);
            writeCollectionData(dsl, parseDocuments(mutableSnapshot, dsl, data.documents));
        	lastRootRowIUsed = sqlInterface.getReadInterface().getLastRowIdUsed(dsl, metaDatabase, metaCollection, rootMetaDocPart);
        	lastSubDocRowIUsed = sqlInterface.getReadInterface().getLastRowIdUsed(dsl, metaDatabase, metaCollection, subDocMetaDocPart);
        	assertEquals(1, lastRootRowIUsed);
        	assertEquals(0, lastSubDocRowIUsed);
        	
            writeCollectionData(dsl, parseDocuments(mutableSnapshot, dsl, data.getMoreDocuments()));
        	lastRootRowIUsed = sqlInterface.getReadInterface().getLastRowIdUsed(dsl, metaDatabase, metaCollection, rootMetaDocPart);
        	lastSubDocRowIUsed = sqlInterface.getReadInterface().getLastRowIdUsed(dsl, metaDatabase, metaCollection, subDocMetaDocPart);
        	assertEquals(2, lastRootRowIUsed);
        	assertEquals(1, lastSubDocRowIUsed);
        	connection.commit();
        }
    }
    
    @Test
    public void testTorodbInsertDocPartData() throws Exception {
        buildMetaSnapshot();
        
        try (Connection connection = sqlInterface.getDbBackend().createWriteConnection()) {
            DSLContext dsl = sqlInterface.getDslContextFactory().createDSLContext(connection);
            
            sqlInterface.getStructureInterface().createSchema(dsl, data.database.getIdentifier());
            createDocPartTable(dsl, data.collection, data.rootDocPart);
            createDocPartTable(dsl, data.collection, data.subDocPart);
            MutableMetaSnapshot mutableSnapshot = new WrapperMutableMetaSnapshot(data.snapshot);
            writeCollectionData(dsl, parseDocuments(mutableSnapshot, dsl, data.documents));
            connection.commit();
            
            StringBuilder rootDocPartSelectStatementBuilder = new StringBuilder("SELECT ");
            data.rootDocPart.streamFields().forEach(metaField ->
                rootDocPartSelectStatementBuilder.append('"')
                    .append(metaField.getIdentifier())
                    .append("\",")
            );
            rootDocPartSelectStatementBuilder.setCharAt(rootDocPartSelectStatementBuilder.length() - 1, ' ');
            rootDocPartSelectStatementBuilder.append("FROM \"")
                    .append(data.database.getIdentifier())
                    .append("\".\"")
                    .append(data.rootDocPart.getIdentifier())
                    .append('"');
            try (PreparedStatement preparedStatement = connection.prepareStatement(rootDocPartSelectStatementBuilder.toString())) {
                ResultSet resultSet = preparedStatement.executeQuery();
                List<Integer> foundRowIndexes = new ArrayList<>();
                while (resultSet.next()) {
                    boolean rowFound = findRootDocPartRow(resultSet, foundRowIndexes);
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
            connection.commit();
        }
    }
    
    @Test
    public void testTorodbDeleteDocParts() throws Exception {
        buildMetaSnapshot();
        
        try (Connection connection = sqlInterface.getDbBackend().createWriteConnection()) {
            DSLContext dsl = sqlInterface.getDslContextFactory().createDSLContext(connection);
            
            sqlInterface.getStructureInterface().createSchema(dsl, data.database.getIdentifier());
            
            createDocPartTable(dsl, data.collection, data.rootDocPart);
            createDocPartTable(dsl, data.collection, data.subDocPart);
            MutableMetaSnapshot mutableSnapshot = new WrapperMutableMetaSnapshot(data.snapshot);
            CollectionData collectionData = parseDocuments(mutableSnapshot, dsl, data.documents);
            List<Integer> generatedDids = writeCollectionData(dsl, collectionData);
            connection.commit();
            
            List<Integer> deletedDids = generatedDids.subList(0, 1);
            sqlInterface.getWriteInterface().deleteDocParts(dsl, data.database.getIdentifier(), data.collection, deletedDids);
            
            StringBuilder rootDocPartSelectStatementBuilder = new StringBuilder()
                    .append("SELECT \"")
                    .append(MetaDocPartTable.DocPartTableFields.DID.fieldName)
                    .append("\" FROM \"")
                    .append(data.database.getIdentifier())
                    .append("\".\"")
                    .append(data.rootDocPart.getIdentifier())
                    .append('"');
            try (PreparedStatement preparedStatement = connection.prepareStatement(rootDocPartSelectStatementBuilder.toString())) {
                ResultSet resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    Integer did = resultSet.getInt(1);
                    Assert.assertFalse(deletedDids.contains(did));
                }
            }
            
            StringBuilder rootSubPartSelectStatementBuilder = new StringBuilder()
                    .append("SELECT \"")
                    .append(MetaDocPartTable.DocPartTableFields.DID.fieldName)
                    .append("\" FROM \"")
                    .append(data.database.getIdentifier())
                    .append("\".\"")
                    .append(data.subDocPart.getIdentifier())
                    .append('"');
            try (PreparedStatement preparedStatement = connection.prepareStatement(rootSubPartSelectStatementBuilder.toString())) {
                ResultSet resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    Integer did = resultSet.getInt(1);
                    Assert.assertFalse(deletedDids.contains(did));
                }
            }
            connection.commit();
        }
    }

	@SuppressWarnings("unchecked")
    private boolean findRootDocPartRow(ResultSet resultSet, List<Integer> foundRowIndexes) throws SQLException {
		Integer index = 0;
		boolean rowFound = true;
		for (KVDocument document : data.documents) {
		    rowFound = true;
		    int columnIndex = 1;
		    Iterator<ImmutableMetaField> rootDocPartFieldsIterator = data.rootDocPart.streamFields().iterator();
            while (rootDocPartFieldsIterator.hasNext())  {
                MetaField metaField = rootDocPartFieldsIterator.next();
		        Optional<KVValue<?>> value;
		        if (document.containsKey(metaField.getName())) {
                    value = Optional.of(document.get(metaField.getName()));
		            if (metaField.getType() == FieldType.CHILD) {
		                if (value.get().getType().equals(DocumentType.INSTANCE)) {
		                    value = Optional.of(KVBoolean.FALSE);
		                } else {
                            value = Optional.of(KVBoolean.TRUE);
		                }
		            }
		        } else {
		            value = Optional.empty();
		        }
		        DataTypeForKV<?> dataTypeForKV = (DataTypeForKV<?>) sqlInterface.getDataTypeProvider().getDataType(metaField.getType());
		        Object databaseValue = sqlHelper.getResultSetValue(FieldType.from(dataTypeForKV.getKVValueConverter().getErasuredType()), resultSet, columnIndex);
		        Optional<KVValue<?>> databaseConvertedValue;
		        if (resultSet.wasNull()) {
		            databaseConvertedValue = Optional.empty();
		        } else {
		            databaseConvertedValue = Optional.of(((Converter<Object, KVValue<?>>) dataTypeForKV.getConverter()).from(databaseValue));
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
        KVDocument document = parseFromJson("testTorodbReadDocPart.json");
        try (Connection connection = sqlInterface.getDbBackend().createWriteConnection()) {
            DSLContext dsl = sqlInterface.getDslContextFactory().createDSLContext(connection);
            MutableMetaSnapshot mutableSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot.Builder().build());
            mutableSnapshot
                .addMetaDatabase(data.database.getName(), data.database.getIdentifier())
                .addMetaCollection(data.collection.getName(), data.collection.getIdentifier());
            sqlInterface.getStructureInterface().createSchema(dsl, data.database.getIdentifier());
            CollectionData collectionData = parseDocumentAndCreateDocPartDataTables(mutableSnapshot, dsl, document);
            
            List<Integer> generatedDids = writeCollectionData(dsl, collectionData);
            
            MetaDatabase metaDatabase = mutableSnapshot.getMetaDatabaseByName(data.database.getName());
            MetaCollection metaCollection = metaDatabase.getMetaCollectionByName(data.collection.getName());
            
            
            DocPartResults<ResultSet> docPartResultSets = sqlInterface.getReadInterface().getCollectionResultSets(
                    dsl, metaDatabase, metaCollection, 
                    new MockDidCursor(generatedDids.iterator()), generatedDids.size());
            
            Collection<ToroDocument> readedDocuments = readDocuments(metaDatabase, metaCollection, docPartResultSets);
            
            KVDocument readedDocument = readedDocuments.iterator().next().getRoot();
            LOGGER.debug(document);
            LOGGER.debug(readedDocument);
            assertEquals(document, readedDocument);
            connection.commit();
        } 
    }
    
    @Test
    public void testTorodbReadAllCollectionResultSetsDids() throws Exception {
        List<KVDocument> documents = parseListFromJson("testTorodbReadDocPartDids.json");
        try (Connection connection = sqlInterface.getDbBackend().createWriteConnection()) {
            DSLContext dsl = sqlInterface.getDslContextFactory().createDSLContext(connection);
            MutableMetaSnapshot mutableSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot.Builder().build());
            mutableSnapshot
                .addMetaDatabase(data.database.getName(), data.database.getIdentifier())
                .addMetaCollection(data.collection.getName(), data.collection.getIdentifier());
            sqlInterface.getStructureInterface().createSchema(dsl, data.database.getIdentifier());
            CollectionData collectionData = parseDocumentsAndCreateDocPartDataTables(mutableSnapshot, dsl, documents);
            
            List<Integer> generatedDids = writeCollectionData(dsl, collectionData);
            
            MetaDatabase metaDatabase = mutableSnapshot.getMetaDatabaseByName(data.database.getName());
            MetaCollection metaCollection = metaDatabase.getMetaCollectionByName(data.collection.getName());
            
            MetaDocPart metaDocPart = metaCollection.getMetaDocPartByTableRef(createTableRef());
            
            Collection<ToroDocument> readedToroDocuments;
            try (
                    DefaultDidCursor defaultDidCursor = new DefaultDidCursor(sqlInterface, sqlInterface.getReadInterface().getAllCollectionDids(dsl, metaDatabase, metaDocPart));
                    DocPartResults<ResultSet> docPartResultSets = sqlInterface.getReadInterface().getCollectionResultSets(
                            dsl, metaDatabase, metaCollection,
                            defaultDidCursor,  generatedDids.size());
                    ) {
                readedToroDocuments = readDocuments(metaDatabase, metaCollection, docPartResultSets);
            }

            List<Integer> readedDids = readedToroDocuments.stream()
                    .map(toroDocument -> toroDocument.getId())
                    .collect(Collectors.toList()); 
            LOGGER.debug(generatedDids);
            LOGGER.debug(readedDids);
            Assert.assertTrue(readedDids.containsAll(generatedDids));
            Assert.assertTrue(generatedDids.containsAll(readedDids));
            List<KVDocument> readedDocuments = readedToroDocuments.stream().map(readedToroDocument -> readedToroDocument.getRoot()).collect(Collectors.toList());
            LOGGER.debug(documents);
            LOGGER.debug(readedDocuments);
            Assert.assertTrue(readedDocuments.containsAll(documents));
            Assert.assertTrue(documents.containsAll(readedDocuments));
            connection.commit();
        } 
    }
    
    @Test
    public void testTorodbReadCollectionResultSetsDidsWithFieldEqualsTo() throws Exception {
        List<KVDocument> documents = parseListFromJson("testTorodbReadDocPartDids.json");
        try (Connection connection = sqlInterface.getDbBackend().createWriteConnection()) {
            DSLContext dsl = sqlInterface.getDslContextFactory().createDSLContext(connection);
            MutableMetaSnapshot mutableSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot.Builder().build());
            mutableSnapshot
                .addMetaDatabase(data.database.getName(), data.database.getIdentifier())
                .addMetaCollection(data.collection.getName(), data.collection.getIdentifier());
            sqlInterface.getStructureInterface().createSchema(dsl, data.database.getIdentifier());
            CollectionData collectionData = parseDocumentsAndCreateDocPartDataTables(mutableSnapshot, dsl, documents);
            
            List<Integer> generatedDids = writeCollectionData(dsl, collectionData);
            
            MetaDatabase metaDatabase = mutableSnapshot.getMetaDatabaseByName(data.database.getName());
            MetaCollection metaCollection = metaDatabase.getMetaCollectionByName(data.collection.getName());
            
            MetaDocPart metaDocPart = metaCollection.getMetaDocPartByTableRef(createTableRef("batters", "batter"));
            MetaField metaField = metaDocPart.getMetaFieldByNameAndType("type", FieldType.STRING);
            
            connection.commit();
            
            Collection<ToroDocument> readedToroDocuments;
            try (
                    DefaultDidCursor defaultDidCursor = new DefaultDidCursor(sqlInterface, sqlInterface.getReadInterface().getCollectionDidsWithFieldEqualsTo(
                            dsl, metaDatabase, metaDocPart, metaField, new StringKVString("Blueberry")));
                    DocPartResults<ResultSet> docPartResultSets = sqlInterface.getReadInterface().getCollectionResultSets(
                            dsl, metaDatabase, metaCollection,
                            defaultDidCursor, generatedDids.size());
                    ) {
                readedToroDocuments = readDocuments(metaDatabase, metaCollection, docPartResultSets);
            }

            Assert.assertEquals(1, readedToroDocuments.size());
            List<Integer> readedDids = readedToroDocuments.stream()
                    .map(toroDocument -> toroDocument.getId())
                    .collect(Collectors.toList()); 
            LOGGER.debug(generatedDids);
            LOGGER.debug(readedDids);
            Assert.assertTrue(generatedDids.containsAll(readedDids));
            List<KVDocument> readedDocuments = readedToroDocuments.stream().map(readedToroDocument -> readedToroDocument.getRoot()).collect(Collectors.toList());
            LOGGER.debug(documents);
            LOGGER.debug(readedDocuments);
            Assert.assertTrue(documents.containsAll(readedDocuments));
            connection.commit();
        } 
    }
    
    @Test
    public void testTorodbReadCollectionResultSetsWithStructures() throws Exception {
        try (Connection connection = sqlInterface.getDbBackend().createWriteConnection()) {
            DSLContext dsl = sqlInterface.getDslContextFactory().createDSLContext(connection);
            MutableMetaSnapshot mutableSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot.Builder().build());
            mutableSnapshot
            	.addMetaDatabase(data.database.getName(), data.database.getIdentifier())
            	.addMetaCollection(data.collection.getName(), data.collection.getIdentifier());
            sqlInterface.getStructureInterface().createSchema(dsl, data.database.getIdentifier());
            
            List<KVDocument> documents = createDocumentsWithStructures();
            
            parseDocumentsAndCreateDocPartDataTables(mutableSnapshot, dsl, documents);
            
            for (KVDocument document : documents) {
                CollectionData collectionData = readDataFromDocuments(data.database.getName(), data.collection.getName(), Arrays.asList(document), mutableSnapshot);
                
                List<Integer> generatedDids = writeCollectionData(dsl, collectionData);
                
                MetaDatabase metaDatabase = mutableSnapshot.getMetaDatabaseByName(data.database.getName());
                MetaCollection metaCollection = metaDatabase.getMetaCollectionByName(data.collection.getName());
                
                DocPartResults<ResultSet> docPartResultSets = sqlInterface.getReadInterface().getCollectionResultSets(
                        dsl, metaDatabase, metaCollection, 
                        new MockDidCursor(generatedDids.iterator()), generatedDids.size());
                
                Collection<ToroDocument> readedDocuments = readDocuments(metaDatabase, metaCollection, docPartResultSets);
                
                KVDocument readedDocument = readedDocuments.iterator().next().getRoot();
                LOGGER.debug("Written :" + document);
                LOGGER.debug("Readed: " + readedDocument);
                assertEquals(document, readedDocument);
            }
            CollectionData collectionData = readDataFromDocuments(data.database.getName(), data.collection.getName(), documents, mutableSnapshot);
            
            List<Integer> generatedDids = writeCollectionData(dsl, collectionData);
            
            MetaDatabase metaDatabase = mutableSnapshot.getMetaDatabaseByName(data.database.getName());
            MetaCollection metaCollection = metaDatabase.getMetaCollectionByName(data.collection.getName());
            
            DocPartResults<ResultSet> docPartResultSets = sqlInterface.getReadInterface().getCollectionResultSets(
                    dsl, metaDatabase, metaCollection, 
                    new MockDidCursor(generatedDids.iterator()), generatedDids.size());
            
            Collection<ToroDocument> readedDocuments = readDocuments(metaDatabase, metaCollection, docPartResultSets);
            LOGGER.debug("Written :" + documents);
            LOGGER.debug("Readed: " + readedDocuments);
            assertEquals(documents.size(), readedDocuments.size());
            connection.commit();
        }
    }

	private List<KVDocument> createDocumentsWithStructures() {
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
		                       documentBuilder.putValue("k", emptyDocument());
		                   else if (object_size == 2)
		                       documentBuilder.putValue("k", singleDocument());
		                   else if (object_size != 0)
		                       continue;
		                }
		                if (current_size > 0) {
		                    current_index = current_index + 1;
		                    List<KVValue<?>> array_value = new ArrayList<>();
		                    if (object_index == current_index) {
		                        if (object_size == 1) 
		                           array_value.add(emptyDocument());
		                        else if (object_size == 2)
		                           array_value.add(singleDocument());
		                        else if (object_size != 0)
		                           continue;
		                    }
							if (array_scalars > 0) {
								if (array_scalars == 1)
									array_value.add(buildArray(1));
								else
									array_value.add(buildArray(1, 2));
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
		                                    array_value.add(buildArray(emptyDocument()));
		                                else if (object_size == 2)
		                                    array_value.add(buildArray(singleDocument()));
		                                else if (object_size != 0)
		                                    array_value = new ArrayList<>(Arrays.asList(new KVValue<?>[] { new KVDocument.Builder()
		                                        .putValue("k", new ListKVArray(array_value))
		                                        .build() }));
		                            }
		                            if (array_scalars > 0) {
		                                if (array_scalars == 1)
		                                   array_value.add(buildArray(1));
		                                else
		                                   array_value.add(buildArray(1, 2));
		                            }
		                        }
		                    }
		                    documentBuilder.putValue("k", new ListKVArray(array_value));
		                }
		                documents.add(documentBuilder.build());
		            }
		        }
		    }
		}
		return documents;
	}
	
	private ListKVArray buildArray(KVValue<?> ... values){
		return new ListKVArray(Arrays.asList(values));
	}
	
	private ListKVArray buildArray(int ... values){
		List<KVValue<?>> ids = IntStream.of(values).mapToObj(i->KVInteger.of(i)).collect(Collectors.toList());
		return new ListKVArray(ids);
	}
	
	private KVDocument emptyDocument(){
		return new KVDocument.Builder().build();
	}
	
	private KVDocument singleDocument(){
		return new KVDocument.Builder()
				.putValue("k", KVInteger.of(1))
				.build();
	}
}
