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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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

import com.google.common.collect.ImmutableList;
import com.torodb.backend.AbstractBackendTest;
import com.torodb.backend.DatabaseInterface;
import com.torodb.backend.DocPartDataImpl;
import com.torodb.backend.DocPartRowImpl;
import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.backend.driver.derby.DerbyDbBackendConfiguration;
import com.torodb.backend.driver.derby.OfficialDerbyDriver;
import com.torodb.core.d2r.CollectionData;
import com.torodb.core.d2r.DocPartResults;
import com.torodb.core.d2r.RidGenerator;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.ImmutableMetaField;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import com.torodb.core.transaction.metainf.WrapperMutableMetaDocPart;
import com.torodb.core.transaction.metainf.WrapperMutableMetaSnapshot;
import com.torodb.d2r.MockRidGenerator;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVInteger;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.kvdocument.values.heap.ListKVArray;

public class BackendDerbyTest extends AbstractBackendTest {
    
    @Test
    public void testTorodbMeta() throws Exception {
        try (Connection connection = dataSource.getConnection()) {
            DerbyTorodbMeta tododbMeta = new DerbyTorodbMeta(DSL.using(connection, SQLDialect.DERBY), tableRefFactory, databaseInterface);
            connection.commit();
            assertFalse(tododbMeta.getCurrentMetaSnapshot().streamMetaDatabases().iterator().hasNext());
        }
        
        try (Connection connection = dataSource.getConnection()) {
        	DerbyTorodbMeta tododbMeta = new DerbyTorodbMeta(DSL.using(connection, SQLDialect.DERBY), tableRefFactory, databaseInterface);
            connection.commit();
            assertFalse(tododbMeta.getCurrentMetaSnapshot().streamMetaDatabases().iterator().hasNext());
        }
    }
    
    @Test
    public void testTorodbMetaStoreAndReload() throws Exception {
        try (Connection connection = dataSource.getConnection()) {
            new DerbyTorodbMeta(DSL.using(connection, SQLDialect.DERBY), tableRefFactory, databaseInterface);
            connection.commit();
        }
        try (Connection connection = dataSource.getConnection()) {
            DSLContext dsl = DSL.using(connection, SQLDialect.DERBY);
            dsl.insertInto(databaseInterface.getMetaDatabaseTable())
                .set(databaseInterface.getMetaDatabaseTable().newRecord().values(databaseName, databaseSchemaName))
                .execute();
            dsl.execute(databaseInterface.createSchemaStatement(databaseSchemaName));
            dsl.insertInto(databaseInterface.getMetaCollectionTable())
                .set(databaseInterface.getMetaCollectionTable().newRecord().values(databaseName, collectionName, collectionIdentifierName))
                .execute();
            dsl.insertInto(databaseInterface.getMetaDocPartTable())
                .set(databaseInterface.getMetaDocPartTable().newRecord().values(databaseName, collectionName, rootDocPartTableRef, rootDocPartTableName))
                .execute();
            for (Map.Entry<String, Field<?>> field : rootDocPartFields.entrySet()) {
                dsl.insertInto(databaseInterface.getMetaFieldTable())
                    .set(databaseInterface.getMetaFieldTable().newRecord().values(databaseName, collectionName, rootDocPartTableRef, 
                            field.getKey(), field.getValue().getName(), 
                            FieldType.from(((DataTypeForKV<?>) field.getValue().getDataType()).getKVValueConverter().getErasuredType())))
                    .execute();
            }
            dsl.execute(databaseInterface.createDocPartTableStatement(dsl.configuration(), databaseSchemaName, rootDocPartTableName, rootDocPartFields.values()));
            dsl.insertInto(databaseInterface.getMetaDocPartTable())
                .set(databaseInterface.getMetaDocPartTable().newRecord().values(databaseName, collectionName, subDocPartTableRef, subDocPartTableName))
                .execute();
            for (Map.Entry<String, Field<?>> field : subDocPartFields.entrySet()) {
                dsl.insertInto(databaseInterface.getMetaFieldTable())
                    .set(databaseInterface.getMetaFieldTable().newRecord().values(databaseName, collectionName, subDocPartTableRef, 
                            field.getKey(), field.getValue().getName(), 
                            FieldType.from(((DataTypeForKV<?>) field.getValue().getDataType()).getKVValueConverter().getErasuredType())))
                    .execute();
            }
            dsl.execute(databaseInterface.createDocPartTableStatement(dsl.configuration(), databaseSchemaName, subDocPartTableName, subDocPartFields.values()));
            connection.commit();
        }
        DerbyTorodbMeta derbyTorodbMeta;
        try (Connection connection = dataSource.getConnection()) {
            derbyTorodbMeta = new DerbyTorodbMeta(DSL.using(connection, SQLDialect.DERBY), tableRefFactory, databaseInterface);
            connection.commit();
        }
        assertNotNull(derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName));
        assertEquals(databaseSchemaName, derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getIdentifier());
        assertNotNull(derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName));
        assertEquals(collectionIdentifierName, derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName)
                .getMetaCollectionByName(collectionName).getIdentifier());
        assertNotNull(derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                .getMetaDocPartByTableRef(rootDocPartTableRef));
        assertEquals(rootDocPartTableName, derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                .getMetaDocPartByTableRef(rootDocPartTableRef).getIdentifier());
        for (Map.Entry<String, Field<?>> field : rootDocPartFields.entrySet()) {
            assertNotNull(derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                    .getMetaDocPartByTableRef(rootDocPartTableRef).getMetaFieldByNameAndType(field.getKey(), 
                            FieldType.from(((DataTypeForKV<?>) field.getValue().getDataType()).getKVValueConverter().getErasuredType())));
            assertEquals(field.getValue().getName(), derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                    .getMetaDocPartByTableRef(rootDocPartTableRef).getMetaFieldByNameAndType(field.getKey(), 
                            FieldType.from(((DataTypeForKV<?>) field.getValue().getDataType()).getKVValueConverter().getErasuredType())).getIdentifier());
        }
        assertNotNull(derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                .getMetaDocPartByTableRef(subDocPartTableRef));
        assertEquals(subDocPartTableName, derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                .getMetaDocPartByTableRef(subDocPartTableRef).getIdentifier());
        for (Map.Entry<String, Field<?>> field : subDocPartFields.entrySet()) {
            assertNotNull(derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                    .getMetaDocPartByTableRef(subDocPartTableRef).getMetaFieldByNameAndType(field.getKey(), 
                            FieldType.from(((DataTypeForKV<?>) field.getValue().getDataType()).getKVValueConverter().getErasuredType())));
            assertEquals(field.getValue().getName(), derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                    .getMetaDocPartByTableRef(subDocPartTableRef).getMetaFieldByNameAndType(field.getKey(), 
                            FieldType.from(((DataTypeForKV<?>) field.getValue().getDataType()).getKVValueConverter().getErasuredType())).getIdentifier());
        }
        
        try (Connection connection = dataSource.getConnection()) {
            DSLContext dsl = DSL.using(connection, SQLDialect.DERBY);
            for (Map.Entry<String, Field<?>> field : newSubDocPartFields.entrySet()) {
                dsl.insertInto(databaseInterface.getMetaFieldTable())
                    .set(databaseInterface.getMetaFieldTable().newRecord().values(databaseName, collectionName, subDocPartTableRef, 
                            field.getKey(), field.getValue().getName(), 
                            FieldType.from(((DataTypeForKV<?>) field.getValue().getDataType()).getKVValueConverter().getErasuredType())))
                    .execute();
                dsl.execute(databaseInterface.addColumnToDocPartTableStatement(dsl.configuration(), databaseSchemaName, subDocPartTableName, field.getValue()));
            }
            connection.commit();
        }
        
        try (Connection connection = dataSource.getConnection()) {
            derbyTorodbMeta = new DerbyTorodbMeta(DSL.using(connection, SQLDialect.DERBY), tableRefFactory, databaseInterface);
            connection.commit();
        }
        assertNotNull(derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName));
        assertEquals(databaseSchemaName, derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getIdentifier());
        assertNotNull(derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName));
        assertEquals(collectionIdentifierName, derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName)
                .getMetaCollectionByName(collectionName).getIdentifier());
        assertNotNull(derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                .getMetaDocPartByTableRef(rootDocPartTableRef));
        assertEquals(rootDocPartTableName, derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                .getMetaDocPartByTableRef(rootDocPartTableRef).getIdentifier());
        for (Map.Entry<String, Field<?>> field : rootDocPartFields.entrySet()) {
            assertNotNull(derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                    .getMetaDocPartByTableRef(rootDocPartTableRef).getMetaFieldByNameAndType(field.getKey(), 
                            FieldType.from(((DataTypeForKV<?>) field.getValue().getDataType()).getKVValueConverter().getErasuredType())));
            assertEquals(field.getValue().getName(), derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                    .getMetaDocPartByTableRef(rootDocPartTableRef).getMetaFieldByNameAndType(field.getKey(), 
                            FieldType.from(((DataTypeForKV<?>) field.getValue().getDataType()).getKVValueConverter().getErasuredType())).getIdentifier());
        }
        assertNotNull(derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                .getMetaDocPartByTableRef(subDocPartTableRef));
        assertEquals(subDocPartTableName, derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                .getMetaDocPartByTableRef(subDocPartTableRef).getIdentifier());
        for (Map.Entry<String, Field<?>> field : subDocPartFields.entrySet()) {
            assertNotNull(derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                    .getMetaDocPartByTableRef(subDocPartTableRef).getMetaFieldByNameAndType(field.getKey(), 
                            FieldType.from(((DataTypeForKV<?>) field.getValue().getDataType()).getKVValueConverter().getErasuredType())));
            assertEquals(field.getValue().getName(), derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                    .getMetaDocPartByTableRef(subDocPartTableRef).getMetaFieldByNameAndType(field.getKey(), 
                            FieldType.from(((DataTypeForKV<?>) field.getValue().getDataType()).getKVValueConverter().getErasuredType())).getIdentifier());
        }
        for (Map.Entry<String, Field<?>> field : newSubDocPartFields.entrySet()) {
            assertNotNull(derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                    .getMetaDocPartByTableRef(subDocPartTableRef).getMetaFieldByNameAndType(field.getKey(), 
                            FieldType.from(((DataTypeForKV<?>) field.getValue().getDataType()).getKVValueConverter().getErasuredType())));
            assertEquals(field.getValue().getName(), derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                    .getMetaDocPartByTableRef(subDocPartTableRef).getMetaFieldByNameAndType(field.getKey(), 
                            FieldType.from(((DataTypeForKV<?>) field.getValue().getDataType()).getKVValueConverter().getErasuredType())).getIdentifier());
        }
    }
    
    @Test
    public void testConsumeRids() throws Exception {
        try (Connection connection = dataSource.getConnection()) {
            new DerbyTorodbMeta(DSL.using(connection, SQLDialect.DERBY), tableRefFactory, databaseInterface);
            connection.commit();
        }
        try (Connection connection = dataSource.getConnection()) {
            DSLContext dsl = DSL.using(connection, SQLDialect.DERBY);
            dsl.insertInto(databaseInterface.getMetaDatabaseTable())
                .set(databaseInterface.getMetaDatabaseTable().newRecord().values(databaseName, databaseSchemaName))
                .execute();
            dsl.execute(databaseInterface.createSchemaStatement(databaseSchemaName));
            dsl.insertInto(databaseInterface.getMetaCollectionTable())
                .set(databaseInterface.getMetaCollectionTable().newRecord().values(databaseName, collectionName, collectionIdentifierName))
                .execute();
            dsl.insertInto(databaseInterface.getMetaDocPartTable())
                .set(databaseInterface.getMetaDocPartTable().newRecord().values(databaseName, collectionName, rootDocPartTableRef, rootDocPartTableName))
                .execute();
            int first100RootRid = databaseInterface.consumeRids(dsl, databaseName, collectionName, rootDocPartTableRef, 100);
            assertEquals(0, first100RootRid);
            int next100RootRid = databaseInterface.consumeRids(dsl, databaseName, collectionName, rootDocPartTableRef, 100);
            assertEquals(100, next100RootRid);
            dsl.execute(databaseInterface.createDocPartTableStatement(dsl.configuration(), databaseSchemaName, rootDocPartTableName, rootDocPartFields.values()));
            dsl.insertInto(databaseInterface.getMetaDocPartTable())
                .set(databaseInterface.getMetaDocPartTable().newRecord().values(databaseName, collectionName, subDocPartTableRef, subDocPartTableName))
                .execute();
            int first100SubRid = databaseInterface.consumeRids(dsl, databaseName, collectionName, subDocPartTableRef, 100);
            assertEquals(0, first100SubRid);
            int next100SubRid = databaseInterface.consumeRids(dsl, databaseName, collectionName, subDocPartTableRef, 100);
            assertEquals(100, next100SubRid);
            connection.commit();
        }
    }
    
    @Test
    public void testTorodbInsertDocPart() throws Exception {
        try (Connection connection = dataSource.getConnection()) {
            new DerbyTorodbMeta(DSL.using(connection, SQLDialect.DERBY), tableRefFactory, databaseInterface);
            connection.commit();
        }
        try (Connection connection = dataSource.getConnection()) {
            DSLContext dsl = DSL.using(connection, SQLDialect.DERBY);
            dsl.execute(databaseInterface.createSchemaStatement(databaseSchemaName));
            ImmutableMetaDocPart.Builder rootMetaDocPartBuilder = new ImmutableMetaDocPart.Builder(rootDocPartTableRef, rootDocPartTableName);
            for (Map.Entry<String, Field<?>> field : rootDocPartFields.entrySet()) {
                rootMetaDocPartBuilder.add(new ImmutableMetaField(field.getKey(), field.getValue().getName(), 
                        FieldType.from(((DataTypeForKV<?>) field.getValue().getDataType()).getKVValueConverter().getErasuredType())));
            }
            ImmutableMetaDocPart rootMetaDocPart = rootMetaDocPartBuilder.build();
        	RidGenerator ridGenerator = new MockRidGenerator();
            DocPartDataImpl docPartData = new DocPartDataImpl(new WrapperMutableMetaDocPart(rootMetaDocPart, w -> {}), 
            		ridGenerator.getDocPartRidGenerator(databaseName, collectionName));
            dsl.execute(databaseInterface.createDocPartTableStatement(dsl.configuration(), databaseSchemaName, rootDocPartTableName, 
                    ImmutableList.<Field<?>>builder()
                        .addAll(databaseInterface.getDocPartTableInternalFields(rootMetaDocPart))
                        .addAll(rootDocPartFields.values())
                        .build()));
            for (Map<String, Optional<KVValue<?>>> rootDocPartValueMap : rootDocPartValues) {
                DocPartRowImpl row = docPartData.appendRootRow();
                for (Map.Entry<String, Optional<KVValue<?>>> rootDocPartValue : rootDocPartValueMap.entrySet()) {
                    if (rootDocPartValue.getValue().isPresent()) {
                        Field<?> field = rootDocPartFields.get(rootDocPartValue.getKey());
                        docPartData.appendColumnValue(row, rootDocPartValue.getKey(), field.getName(), 
                            FieldType.from(((DataTypeForKV<?>) field.getDataType()).getKVValueConverter().getErasuredType()), 
                            rootDocPartValue.getValue().get());
                    }
                }
            }
            databaseInterface.insertDocPartData(dsl, databaseSchemaName, docPartData);
            connection.commit();
            StringBuilder rootDocPartSelectStatementBuilder = new StringBuilder("SELECT ");
            for (Map.Entry<String, Field<?>> field : rootDocPartFields.entrySet()) {
                rootDocPartSelectStatementBuilder.append('"')
                    .append(field.getValue().getName())
                    .append("\",");
            }
            rootDocPartSelectStatementBuilder.setCharAt(rootDocPartSelectStatementBuilder.length() - 1, ' ');
            rootDocPartSelectStatementBuilder.append("FROM \"")
                    .append(databaseSchemaName)
                    .append("\".\"")
                    .append(rootDocPartTableName)
                    .append('"');
            try (PreparedStatement preparedStatement = connection.prepareStatement(rootDocPartSelectStatementBuilder.toString())) {
                ResultSet resultSet = preparedStatement.executeQuery();
                List<Integer> foundRowIndexes = new ArrayList<>();
                while (resultSet.next()) {
                    Integer index = 0;
                    boolean rowFound = true;
                    for (Map<String, Optional<KVValue<?>>> rootDocPartValueMap : rootDocPartValues) {
                        rowFound = true;
                        int columnIndex = 1;
                        for (Map.Entry<String, Field<?>> field : rootDocPartFields.entrySet()) {
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
    
    @Test
    public void testTorodbReadCollectionResultSets() throws Exception {
        KVDocument document = parseFromJson("testTorodbReadDocPart.json");
        MutableMetaSnapshot mutableSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot.Builder().build());
        mutableSnapshot.addMetaDatabase(databaseName, databaseSchemaName).addMetaCollection(collectionName, collectionIdentifierName);
        try (Connection connection = dataSource.getConnection()) {
            DSLContext dsl = DSL.using(connection, SQLDialect.DERBY);
            dsl.execute(databaseInterface.createSchemaStatement(databaseSchemaName));
            CollectionData collectionData = writeDocumentMeta(mutableSnapshot, dsl, document);
            
            List<Integer> generatedDids = writeCollectionData(dsl, collectionData);
            
            MetaDatabase metaDatabase = mutableSnapshot
                    .getMetaDatabaseByName(databaseName);
            MetaCollection metaCollection = metaDatabase
                    .getMetaCollectionByName(collectionName);
            
            DocPartResults<ResultSet> docPartResultSets = databaseInterface.getCollectionResultSets(
                    dsl, metaDatabase, metaCollection, 
                    generatedDids.toArray(new Integer[generatedDids.size()]));
            
            Collection<KVDocument> readedDocuments = readDocuments(metaDatabase, metaCollection, docPartResultSets);
            
            KVDocument readedDocument = readedDocuments.iterator().next();
            System.out.println(document);
            System.out.println(readedDocument);
            assertEquals(document, readedDocument);
        }
    }
    
    @Test
    public void testTorodbReadCollectionResultSetsWithStructures() throws Exception {
        try (Connection connection = dataSource.getConnection()) {
            DSLContext dsl = DSL.using(connection, SQLDialect.DERBY);
            MutableMetaSnapshot mutableSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot.Builder().build());
            mutableSnapshot.addMetaDatabase(databaseName, databaseSchemaName).addMetaCollection(collectionName, collectionIdentifierName);
            dsl.execute(databaseInterface.createSchemaStatement(databaseSchemaName));
            
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
            
            writeDocumentsMeta(mutableSnapshot, dsl, documents);
            
            for (KVDocument document : documents) {
                CollectionData collectionData = readDataFromDocument(databaseName, collectionName, document, mutableSnapshot);
                
                List<Integer> generatedDids = writeCollectionData(dsl, collectionData);
                
                MetaDatabase metaDatabase = mutableSnapshot
                        .getMetaDatabaseByName(databaseName);
                MetaCollection metaCollection = metaDatabase
                        .getMetaCollectionByName(collectionName);
                
                DocPartResults<ResultSet> docPartResultSets = databaseInterface.getCollectionResultSets(
                        dsl, metaDatabase, metaCollection, 
                        generatedDids.toArray(new Integer[generatedDids.size()]));
                
                Collection<KVDocument> readedDocuments = readDocuments(metaDatabase, metaCollection, docPartResultSets);
                
                KVDocument readedDocument = readedDocuments.iterator().next();
                System.out.println("Written :" + document);
                System.out.println("Readed: " + readedDocument);
                assertEquals(document, readedDocument);
            }
            CollectionData collectionData = readDataFromDocuments(databaseName, collectionName, documents, mutableSnapshot);
            
            List<Integer> generatedDids = writeCollectionData(dsl, collectionData);
            
            MetaDatabase metaDatabase = mutableSnapshot
                    .getMetaDatabaseByName(databaseName);
            MetaCollection metaCollection = metaDatabase
                    .getMetaCollectionByName(collectionName);
            
            DocPartResults<ResultSet> docPartResultSets = databaseInterface.getCollectionResultSets(
                    dsl, metaDatabase, metaCollection, 
                    generatedDids.toArray(new Integer[generatedDids.size()]));
            
            Collection<KVDocument> readedDocuments = readDocuments(metaDatabase, metaCollection, docPartResultSets);
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
        OfficialDerbyDriver derbyDriver = new OfficialDerbyDriver();
        return derbyDriver.getConfiguredDataSource(new DerbyDbBackendConfiguration() {
            @Override
            public String getUsername() {
                return null;
            }
            
            @Override
            public int getReservedReadPoolSize() {
                return 0;
            }
            
            @Override
            public String getPassword() {
                return null;
            }
            
            @Override
            public int getDbPort() {
                return 0;
            }
            
            @Override
            public String getDbName() {
                return "torodb";
            }
            
            @Override
            public String getDbHost() {
                return null;
            }
            
            @Override
            public long getCursorTimeout() {
                return 0;
            }
            
            @Override
            public long getConnectionPoolTimeout() {
                return 0;
            }
            
            @Override
            public int getConnectionPoolSize() {
                return 0;
            }

            @Override
            public boolean inMemory() {
                return false;
            }

            @Override
            public boolean embedded() {
                return true;
            }
        }, "torod");
    }
}
