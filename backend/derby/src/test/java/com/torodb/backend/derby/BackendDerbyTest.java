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

import static org.junit.Assert.assertFalse;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.StreamSupport;

import javax.sql.DataSource;

import org.jooq.Converter;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.torodb.backend.DocPartDataImpl;
import com.torodb.backend.DocPartRidGenerator;
import com.torodb.backend.DocPartRowImpl;
import com.torodb.backend.InternalField;
import com.torodb.backend.TableRefComparator;
import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.backend.driver.derby.DerbyDbBackendConfiguration;
import com.torodb.backend.driver.derby.OfficialDerbyDriver;
import com.torodb.backend.interfaces.ReadInterface.DocPartResultSet;
import com.torodb.backend.meta.TorodbSchema;
import com.torodb.backend.mocks.ToroImplementationException;
import com.torodb.backend.tables.MetaDocPartTable;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.d2r.CollectionData;
import com.torodb.core.d2r.D2RTranslator;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.impl.TableRefFactoryImpl;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.ImmutableMetaField;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import com.torodb.core.transaction.metainf.WrapperMutableMetaDocPart;
import com.torodb.core.transaction.metainf.WrapperMutableMetaSnapshot;
import com.torodb.d2r.D2RTranslatorStack;
import com.torodb.d2r.IdentifierFactoryImpl;
import com.torodb.d2r.MockRidGenerator;
import com.torodb.kvdocument.conversion.json.JacksonJsonParser;
import com.torodb.kvdocument.conversion.json.JsonParser;
import com.torodb.kvdocument.values.KVBoolean;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVDouble;
import com.torodb.kvdocument.values.KVInteger;
import com.torodb.kvdocument.values.KVLong;
import com.torodb.kvdocument.values.KVNull;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.kvdocument.values.heap.ByteArrayKVMongoObjectId;
import com.torodb.kvdocument.values.heap.DefaultKVMongoTimestamp;
import com.torodb.kvdocument.values.heap.ListKVArray;
import com.torodb.kvdocument.values.heap.LocalDateKVDate;
import com.torodb.kvdocument.values.heap.LocalTimeKVTime;
import com.torodb.kvdocument.values.heap.LongKVInstant;
import com.torodb.kvdocument.values.heap.StringKVString;

public class BackendDerbyTest {
    
    private static final TableRefFactory tableRefFactory = new TableRefFactoryImpl();
    
    private DerbyDatabaseInterface databaseInterface;
    private String databaseName;
    private String databaseSchemaName;
    private String collectionName;
    private String collectionIdentifierName;
    private TableRef rootDocPartTableRef;
    private String rootDocPartTableName;
    private ImmutableMap<String, Field<?>> rootDocPartFields;
    private TableRef subDocPartTableRef;
    private String subDocPartTableName;
    private ImmutableMap<String, Field<?>> subDocPartFields;
    private ImmutableMap<String, Field<?>> newSubDocPartFields;
    private ImmutableList<ImmutableMap<String, Optional<KVValue<?>>>> rootDocPartValues;
    private JsonParser parser = new JacksonJsonParser();
    
    private DataSource dataSource;
    
    @Before
    public void setUp() throws Exception {
        databaseInterface = new DerbyDatabaseInterface(tableRefFactory);
        databaseName = "databaseName";
        databaseSchemaName = "databaseSchemaName";
        collectionName = "collectionName";
        collectionIdentifierName = "collectionIdentifierName";
        rootDocPartTableRef = tableRefFactory.createRoot();
        rootDocPartTableName = "rootDocPartTableName";
        rootDocPartFields = ImmutableMap.<String, Field<?>>builder()
                .put("nullRoot", DSL.field("nullRootField", databaseInterface.getDataType(FieldType.NULL)))
                .put("booleanRoot", DSL.field("booleanRootField", databaseInterface.getDataType(FieldType.BOOLEAN)))
                .put("integerRoot", DSL.field("integerRootField", databaseInterface.getDataType(FieldType.INTEGER)))
                .put("longRoot", DSL.field("longRootField", databaseInterface.getDataType(FieldType.LONG)))
                .put("doubleRoot", DSL.field("doubleRootField", databaseInterface.getDataType(FieldType.DOUBLE)))
                .put("stringRoot", DSL.field("stringRootField", databaseInterface.getDataType(FieldType.STRING)))
                .put("dateRoot", DSL.field("dateRootField", databaseInterface.getDataType(FieldType.DATE)))
                .put("timeRoot", DSL.field("timeRootField", databaseInterface.getDataType(FieldType.TIME)))
                .put("mongoObjectIdRoot", DSL.field("mongoObjectIdRootField", databaseInterface.getDataType(FieldType.MONGO_OBJECT_ID)))
                .put("mongoTimeStampRoot", DSL.field("mongoTimeStampRootField", databaseInterface.getDataType(FieldType.MONGO_TIME_STAMP)))
                .put("instantRoot", DSL.field("instantRootField", databaseInterface.getDataType(FieldType.INSTANT)))
                .put("subDocPart", DSL.field("subDocPartField", databaseInterface.getDataType(FieldType.CHILD)))
                .build();
        subDocPartTableRef = tableRefFactory.createChild(rootDocPartTableRef, "subDocPart");
        subDocPartTableName = "subDocPartTableName";
        subDocPartFields = ImmutableMap.<String, Field<?>>builder()
                .put("nullSub", DSL.field("nullSubField", databaseInterface.getDataType(FieldType.NULL)))
                .put("booleanSub", DSL.field("booleanSubField", databaseInterface.getDataType(FieldType.BOOLEAN)))
                .put("integerSub", DSL.field("integerSubField", databaseInterface.getDataType(FieldType.INTEGER)))
                .put("longSub", DSL.field("longSubField", databaseInterface.getDataType(FieldType.LONG)))
                .put("doubleSub", DSL.field("doubleSubField", databaseInterface.getDataType(FieldType.DOUBLE)))
                .put("stringSub", DSL.field("stringSubField", databaseInterface.getDataType(FieldType.STRING)))
                .put("dateSub", DSL.field("dateSubField", databaseInterface.getDataType(FieldType.DATE)))
                .put("timeSub", DSL.field("timeSubField", databaseInterface.getDataType(FieldType.TIME)))
                .put("mongoObjectIdSub", DSL.field("mongoObjectIdSubField", databaseInterface.getDataType(FieldType.MONGO_OBJECT_ID)))
                .put("mongoTimeStampSub", DSL.field("mongoTimeStampSubField", databaseInterface.getDataType(FieldType.MONGO_TIME_STAMP)))
                .put("instantSub", DSL.field("instantSubField", databaseInterface.getDataType(FieldType.INSTANT)))
                .build();
        newSubDocPartFields = ImmutableMap.<String, Field<?>>builder()
                .put("newNullSub", DSL.field("newNullSubField", databaseInterface.getDataType(FieldType.NULL)))
                .put("newBooleanSub", DSL.field("newBooleanSubField", databaseInterface.getDataType(FieldType.BOOLEAN)))
                .put("newIntegerSub", DSL.field("newIntegerSubField", databaseInterface.getDataType(FieldType.INTEGER)))
                .put("newLongSub", DSL.field("newLongSubField", databaseInterface.getDataType(FieldType.LONG)))
                .put("newDoubleSub", DSL.field("newDoubleSubField", databaseInterface.getDataType(FieldType.DOUBLE)))
                .put("newStringSub", DSL.field("newStringSubField", databaseInterface.getDataType(FieldType.STRING)))
                .put("newDateSub", DSL.field("newDateSubField", databaseInterface.getDataType(FieldType.DATE)))
                .put("newTimeSub", DSL.field("newTimeSubField", databaseInterface.getDataType(FieldType.TIME)))
                .put("newMongoObjectIdSub", DSL.field("newMongoObjectIdSubField", databaseInterface.getDataType(FieldType.MONGO_OBJECT_ID)))
                .put("newMongoTimeStampSub", DSL.field("newMongoTimeStampSubField", databaseInterface.getDataType(FieldType.MONGO_TIME_STAMP)))
                .put("newInstantSub", DSL.field("newInstantSubField", databaseInterface.getDataType(FieldType.INSTANT)))
                .build();
        rootDocPartValues = ImmutableList.<ImmutableMap<String, Optional<KVValue<?>>>>builder()
                .add(ImmutableMap.<String, Optional<KVValue<?>>>builder()
                        .put("nullRoot", Optional.of(KVNull.getInstance()))
                        .put("booleanRoot", Optional.of(KVBoolean.TRUE))
                        .put("integerRoot", Optional.of(KVInteger.of(1)))
                        .put("longRoot", Optional.of(KVLong.of(2)))
                        .put("doubleRoot", Optional.of(KVDouble.of(3.3)))
                        .put("stringRoot", Optional.of(new StringKVString("Lorem ipsum")))
                        .put("dateRoot", Optional.of(new LocalDateKVDate(LocalDate.of(2016, 06, 7))))
                        .put("timeRoot", Optional.of(new LocalTimeKVTime(LocalTime.of(17, 29, 00))))
                        .put("mongoObjectIdRoot", Optional.of(new ByteArrayKVMongoObjectId(
                                new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})))
                        .put("mongoTimeStampRoot", Optional.of(new DefaultKVMongoTimestamp(0, 0)))
                        .put("instantRoot", Optional.of(new LongKVInstant(0)))
                        .put("subDocPart", Optional.of(KVBoolean.FALSE))
                        .build())
                .add(ImmutableMap.<String, Optional<KVValue<?>>>builder()
                        .put("nullRoot", Optional.empty())
                        .put("booleanRoot", Optional.empty())
                        .put("integerRoot", Optional.empty())
                        .put("longRoot", Optional.empty())
                        .put("doubleRoot", Optional.empty())
                        .put("stringRoot", Optional.empty())
                        .put("dateRoot", Optional.empty())
                        .put("timeRoot", Optional.empty())
                        .put("mongoObjectIdRoot", Optional.empty())
                        .put("mongoTimeStampRoot", Optional.empty())
                        .put("instantRoot", Optional.empty())
                        .put("subDocPart", Optional.empty())
                        .build())
                .build();
        OfficialDerbyDriver derbyDriver = new OfficialDerbyDriver();
        dataSource = derbyDriver.getConfiguredDataSource(new DerbyDbBackendConfiguration() {
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
        try (Connection connection = dataSource.getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet tables = metaData.getTables("%", "%", "%", null);
            while (tables.next()) {
                String schemaName = tables.getString("TABLE_SCHEM");
                String tableName = tables.getString("TABLE_NAME");
                if (!databaseInterface.isRestrictedSchemaName(schemaName) || schemaName.equals(TorodbSchema.TORODB_SCHEMA)) {
                    try (PreparedStatement preparedStatement = connection.prepareStatement("DROP TABLE \"" + schemaName + "\".\"" + tableName + "\"")) {
                        preparedStatement.executeUpdate();
                    }
                }
            }
            ResultSet schemas = metaData.getSchemas();
            while (schemas.next()) {
                String schemaName = schemas.getString("TABLE_SCHEM");
                if (!databaseInterface.isRestrictedSchemaName(schemaName) || schemaName.equals(TorodbSchema.TORODB_SCHEMA)) {
                    try (PreparedStatement preparedStatement = connection.prepareStatement("DROP SCHEMA \"" + schemaName + "\" RESTRICT")) {
                        preparedStatement.executeUpdate();
                    }
                }
            }
            connection.commit();
        }
    }
    
    protected KVDocument parseFromJson(String jsonFileName) throws Exception {
        return parser.createFromResource("docs/" + jsonFileName);
    }
    
    protected CollectionData readDataFromDocument(Connection connection, String database, String collection, KVDocument document, MutableMetaSnapshot mutableSnapshot) throws Exception {
        MockRidGenerator ridGenerator = new MockRidGenerator();
        IdentifierFactory identifierFactory = new IdentifierFactoryImpl();
        D2RTranslator translator = new D2RTranslatorStack(tableRefFactory, identifierFactory, ridGenerator, mutableSnapshot, database, collection);
        translator.translate(document);
        return translator.getCollectionDataAccumulator();
    }
    
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
        Assert.assertNotNull(derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName));
        Assert.assertEquals(databaseSchemaName, derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getIdentifier());
        Assert.assertNotNull(derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName));
        Assert.assertEquals(collectionIdentifierName, derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName)
                .getMetaCollectionByName(collectionName).getIdentifier());
        Assert.assertNotNull(derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                .getMetaDocPartByTableRef(rootDocPartTableRef));
        Assert.assertEquals(rootDocPartTableName, derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                .getMetaDocPartByTableRef(rootDocPartTableRef).getIdentifier());
        for (Map.Entry<String, Field<?>> field : rootDocPartFields.entrySet()) {
            Assert.assertNotNull(derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                    .getMetaDocPartByTableRef(rootDocPartTableRef).getMetaFieldByNameAndType(field.getKey(), 
                            FieldType.from(((DataTypeForKV<?>) field.getValue().getDataType()).getKVValueConverter().getErasuredType())));
            Assert.assertEquals(field.getValue().getName(), derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                    .getMetaDocPartByTableRef(rootDocPartTableRef).getMetaFieldByNameAndType(field.getKey(), 
                            FieldType.from(((DataTypeForKV<?>) field.getValue().getDataType()).getKVValueConverter().getErasuredType())).getIdentifier());
        }
        Assert.assertNotNull(derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                .getMetaDocPartByTableRef(subDocPartTableRef));
        Assert.assertEquals(subDocPartTableName, derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                .getMetaDocPartByTableRef(subDocPartTableRef).getIdentifier());
        for (Map.Entry<String, Field<?>> field : subDocPartFields.entrySet()) {
            Assert.assertNotNull(derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                    .getMetaDocPartByTableRef(subDocPartTableRef).getMetaFieldByNameAndType(field.getKey(), 
                            FieldType.from(((DataTypeForKV<?>) field.getValue().getDataType()).getKVValueConverter().getErasuredType())));
            Assert.assertEquals(field.getValue().getName(), derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
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
        Assert.assertNotNull(derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName));
        Assert.assertEquals(databaseSchemaName, derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getIdentifier());
        Assert.assertNotNull(derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName));
        Assert.assertEquals(collectionIdentifierName, derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName)
                .getMetaCollectionByName(collectionName).getIdentifier());
        Assert.assertNotNull(derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                .getMetaDocPartByTableRef(rootDocPartTableRef));
        Assert.assertEquals(rootDocPartTableName, derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                .getMetaDocPartByTableRef(rootDocPartTableRef).getIdentifier());
        for (Map.Entry<String, Field<?>> field : rootDocPartFields.entrySet()) {
            Assert.assertNotNull(derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                    .getMetaDocPartByTableRef(rootDocPartTableRef).getMetaFieldByNameAndType(field.getKey(), 
                            FieldType.from(((DataTypeForKV<?>) field.getValue().getDataType()).getKVValueConverter().getErasuredType())));
            Assert.assertEquals(field.getValue().getName(), derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                    .getMetaDocPartByTableRef(rootDocPartTableRef).getMetaFieldByNameAndType(field.getKey(), 
                            FieldType.from(((DataTypeForKV<?>) field.getValue().getDataType()).getKVValueConverter().getErasuredType())).getIdentifier());
        }
        Assert.assertNotNull(derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                .getMetaDocPartByTableRef(subDocPartTableRef));
        Assert.assertEquals(subDocPartTableName, derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                .getMetaDocPartByTableRef(subDocPartTableRef).getIdentifier());
        for (Map.Entry<String, Field<?>> field : subDocPartFields.entrySet()) {
            Assert.assertNotNull(derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                    .getMetaDocPartByTableRef(subDocPartTableRef).getMetaFieldByNameAndType(field.getKey(), 
                            FieldType.from(((DataTypeForKV<?>) field.getValue().getDataType()).getKVValueConverter().getErasuredType())));
            Assert.assertEquals(field.getValue().getName(), derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                    .getMetaDocPartByTableRef(subDocPartTableRef).getMetaFieldByNameAndType(field.getKey(), 
                            FieldType.from(((DataTypeForKV<?>) field.getValue().getDataType()).getKVValueConverter().getErasuredType())).getIdentifier());
        }
        for (Map.Entry<String, Field<?>> field : newSubDocPartFields.entrySet()) {
            Assert.assertNotNull(derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
                    .getMetaDocPartByTableRef(subDocPartTableRef).getMetaFieldByNameAndType(field.getKey(), 
                            FieldType.from(((DataTypeForKV<?>) field.getValue().getDataType()).getKVValueConverter().getErasuredType())));
            Assert.assertEquals(field.getValue().getName(), derbyTorodbMeta.getCurrentMetaSnapshot().getMetaDatabaseByName(databaseName).getMetaCollectionByName(collectionName)
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
            Assert.assertEquals(0, first100RootRid);
            int next100RootRid = databaseInterface.consumeRids(dsl, databaseName, collectionName, rootDocPartTableRef, 100);
            Assert.assertEquals(100, next100RootRid);
            dsl.execute(databaseInterface.createDocPartTableStatement(dsl.configuration(), databaseSchemaName, rootDocPartTableName, rootDocPartFields.values()));
            dsl.insertInto(databaseInterface.getMetaDocPartTable())
                .set(databaseInterface.getMetaDocPartTable().newRecord().values(databaseName, collectionName, subDocPartTableRef, subDocPartTableName))
                .execute();
            int first100SubRid = databaseInterface.consumeRids(dsl, databaseName, collectionName, subDocPartTableRef, 100);
            Assert.assertEquals(0, first100SubRid);
            int next100SubRid = databaseInterface.consumeRids(dsl, databaseName, collectionName, subDocPartTableRef, 100);
            Assert.assertEquals(100, next100SubRid);
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
            DocPartDataImpl docPartData = new DocPartDataImpl(new WrapperMutableMetaDocPart(rootMetaDocPart, w -> {}), 
                    new DocPartRidGenerator(databaseName, collectionName, new MockRidGenerator()));
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
    public void testTorodbReadDocPart() throws Exception {
        KVDocument document = parseFromJson("testTorodbReadDocPart.json");
        MutableMetaSnapshot mutableSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot.Builder().build());
        mutableSnapshot.addMetaDatabase(databaseName, databaseSchemaName).addMetaCollection(collectionName, collectionIdentifierName);
        try (Connection connection = dataSource.getConnection()) {
            DSLContext dsl = DSL.using(connection, SQLDialect.DERBY);
            CollectionData collectionData = readDataFromDocument(connection, databaseName, collectionName, document, mutableSnapshot);
            dsl.execute(databaseInterface.createSchemaStatement(databaseSchemaName));
            mutableSnapshot.streamMetaDatabases().forEachOrdered(metaDatabase -> {
                metaDatabase.streamMetaCollections().forEachOrdered(metaCollection -> {
                    metaCollection.streamContainedMetaDocParts().sorted(TableRefComparator.MetaDocPart.ASC).forEachOrdered(metaDocPartObject -> {
                        MetaDocPart metaDocPart = (MetaDocPart) metaDocPartObject;
                        List<Field<?>> fields = new ArrayList<>(databaseInterface.getDocPartTableInternalFields(metaDocPart));
                        metaDocPart.streamFields().forEachOrdered(metaField -> {
                            fields.add(DSL.field(metaField.getIdentifier(), databaseInterface.getDataType(metaField.getType())));
                        });
                        dsl.execute(databaseInterface.createDocPartTableStatement(dsl.configuration(), databaseSchemaName, metaDocPart.getIdentifier(), fields));
                    });
                });
            });
            
            Iterator<DocPartData> docPartDataIterator = StreamSupport.stream(collectionData.spliterator(), false)
                    .iterator();
            List<Integer> generatedDids = new ArrayList<>();
            while (docPartDataIterator.hasNext()) {
                DocPartData docPartData = docPartDataIterator.next();
                if (docPartData.getMetaDocPart().getTableRef().isRoot()) {
                    docPartData.forEach(docPartRow -> {
                        generatedDids.add(docPartRow.getDid());
                    });
                }
                databaseInterface.insertDocPartData(dsl, databaseSchemaName, docPartData);
            }
            
            MetaDatabase metaDatabase = mutableSnapshot
                    .getMetaDatabaseByName(databaseName);
            MetaCollection metaCollection = metaDatabase
                    .getMetaCollectionByName(collectionName);
            
            Collection<DocPartResultSet> colelctionResultSets = databaseInterface.getCollectionResultSets(
                    dsl, metaDatabase, metaCollection, 
                    generatedDids.toArray(new Integer[generatedDids.size()]));
            
            Converter<Object, Integer> didConverter = (Converter<Object, Integer>) 
                    databaseInterface.getMetaDocPartTable().DID.getConverter();
            
            Map<Integer, KVDocument> readedDocuments = Maps.newHashMap();
            Table<TableRef, String, Map<Integer, List<KVValue<?>>>> currentDocPartTable = 
                    HashBasedTable.<TableRef, String, Map<Integer, List<KVValue<?>>>>create();
            Table<TableRef, String, Map<Integer, List<KVValue<?>>>> childDocPartTable = 
                    HashBasedTable.<TableRef, String, Map<Integer, List<KVValue<?>>>>create();
            int previousLevel = -1;
            Iterator<DocPartResultSet> docPartResultSetIterator = colelctionResultSets.stream()
                    .sorted(TableRefComparator.DocPartResultSet.DESC).iterator();
            while(docPartResultSetIterator.hasNext()) {
                DocPartResultSet docPartResultSet = docPartResultSetIterator.next();
                MetaDocPart metaDocPart = docPartResultSet.getMetaDocPart();
                TableRef tableRef = metaDocPart.getTableRef();
                
                if (previousLevel == -1 || previousLevel != tableRef.getDepth()) {
                    Table<TableRef, String, Map<Integer, List<KVValue<?>>>> 
                        previousChildDocPartTable = childDocPartTable;
                    childDocPartTable = currentDocPartTable;
                    currentDocPartTable = previousChildDocPartTable;
                    currentDocPartTable.clear();
                }
                previousLevel = tableRef.getDepth();
                
                Map<String, Map<Integer, List<KVValue<?>>>> childDocPartRow = 
                        childDocPartTable.row(tableRef);
                
                ResultSet resultSet = docPartResultSet.getResultSet();
                while (resultSet.next()) {
                    Integer did = null;
                    Integer pid = null;
                    Integer rid = null;
                    Integer seq = null;
                    Collection<InternalField<?>> internalFields = databaseInterface.getDocPartTableInternalFields(
                            metaDocPart);
                    int columnIndex = 1;
                    for (InternalField<?> internalField : internalFields) {
                        if (internalField.isDid()) {
                            did = didConverter.from(resultSet.getObject(columnIndex));
                        } else if (internalField.isRid()) {
                            rid = didConverter.from(resultSet.getObject(columnIndex));
                        } else if (internalField.isPid()) {
                            pid = didConverter.from(resultSet.getObject(columnIndex));
                        } else if (internalField.isSeq()) {
                            seq = didConverter.from(resultSet.getObject(columnIndex));
                        }
                        columnIndex++;
                    }
                    if (did == null) {
                        throw new ToroImplementationException("did was not found for doc part " + tableRef 
                                + " in collection " + collectionName + " and database " + databaseName);
                    }
                    
                    if (rid == null) {
                        rid = did;
                    }
                    
                    if (pid == null) {
                        pid = did;
                    }
                    
                    KVDocument.Builder documentBuilder = new KVDocument.Builder();
                    //TODO: ensure MetaField order using ResultSet meta data
                    Iterator<? extends MetaField> metaFieldIterator = metaDocPart
                            .streamFields().iterator();
                    boolean wasScalar = false;
                    while (metaFieldIterator.hasNext()) {
                        MetaField metaField = metaFieldIterator.next();
                        Object databaseValue = resultSet.getObject(columnIndex);
                        columnIndex++;
                        
                        if (databaseValue == null) {
                            continue;
                        }
                        
                        DataTypeForKV<?> dataType = databaseInterface.getDataType(metaField.getType());
                        Converter<Object, KVValue<?>> converter = (Converter<Object, KVValue<?>>) dataType.getConverter();
                        KVValue<?> value = converter.from(databaseValue);
                        
                        if (metaField.getType() == FieldType.CHILD) {
                            KVBoolean child = (KVBoolean) value;
                            Map<Integer, List<KVValue<?>>> childDocPartCell = childDocPartRow.get(metaField.getName());
                            if (child.getValue()) {
                                if (childDocPartCell == null) {
                                    value = new ListKVArray(ImmutableList.of());
                                } else {
                                    value = new ListKVArray(childDocPartCell.get(rid));
                                }
                            } else {
                                value = childDocPartCell.get(rid).get(0);
                            }
                        }
                        
                        if(metaField.getIdentifier().indexOf(MetaDocPartTable.DocPartTableFields.SCALAR.fieldName) == 0
                                && metaField.getIdentifier().length() == MetaDocPartTable.DocPartTableFields.SCALAR.fieldName.length() + 2) {
                            assert !tableRef.isRoot() : "found scalar value in root doc part";
                            Map<Integer, List<KVValue<?>>> currentDocPartCell = getDocPartCell(
                                    currentDocPartTable, tableRef, seq);
                            if (seq == null) {
                                currentDocPartCell.put(pid, ImmutableList.of(value));
                            } else {
                                List<KVValue<?>> elements = getCellElements(currentDocPartCell, pid);
                                setElementValue(elements, seq, value);
                            }
                            wasScalar = true;
                            break;
                        } else {
                            documentBuilder.putValue(metaField.getName(), value);
                        }
                    }
                    
                    if (wasScalar) {
                        continue;
                    }
                    
                    if (tableRef.isRoot()) {
                        readedDocuments.put(did, documentBuilder.build());
                    } else {
                        Map<Integer, List<KVValue<?>>> currentDocPartCell = getDocPartCell(
                                currentDocPartTable, tableRef, seq);
                        if (seq == null) {
                            currentDocPartCell.put(pid, ImmutableList.of(documentBuilder.build()));
                        } else {
                            List<KVValue<?>> elements = getCellElements(currentDocPartCell, pid);
                            setElementValue(elements, seq, documentBuilder.build());
                        }
                    }
                }
            }
            
            KVDocument readedDocument = readedDocuments.values().iterator().next();
            System.out.println(document);
            System.out.println(readedDocument);
            Assert.assertEquals(document, readedDocument);
        }
    }

    private Map<Integer, List<KVValue<?>>> getDocPartCell(
            Table<TableRef, String, Map<Integer, List<KVValue<?>>>> docPartTable, TableRef tableRef, Integer seq) {
        String name = tableRef.getName();
        
        if (seq != null && tableRef.isInArray()) {
            name = MetaDocPartTable.DocPartTableFields.SCALAR.fieldName;
        }
        
        return getDocPartCell(docPartTable, tableRef, name);
    }

    private Map<Integer, List<KVValue<?>>> getDocPartCell(
            Table<TableRef, String, Map<Integer, List<KVValue<?>>>> docPartTable, TableRef tableRef, String name) {
        Map<Integer, List<KVValue<?>>> docPartCell = 
                docPartTable.get(tableRef.getParent().get(), name);
        if (docPartCell == null) {
            docPartCell = new HashMap<>();
            docPartTable.put(tableRef.getParent().get(), name, docPartCell);
        }
        return docPartCell;
    }

    private List<KVValue<?>> getCellElements(Map<Integer, List<KVValue<?>>> docPartCell, Integer pid) {
        List<KVValue<?>> elements = docPartCell.get(pid);
        if (elements == null) {
            elements = new ArrayList<>();
            docPartCell.put(pid, elements);
        }
        return elements;
    }

    private void setElementValue(List<KVValue<?>> elements, Integer seq, KVValue<?> value) {
        if (seq >= elements.size()) {
            for (int i=elements.size(); i<=seq; i++) {
                elements.add(null);
            }
        }
        elements.set(seq, value);
    }
    
}
