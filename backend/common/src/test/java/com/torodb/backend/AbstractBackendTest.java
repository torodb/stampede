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

package com.torodb.backend;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.StreamSupport;

import javax.sql.DataSource;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.junit.Before;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.torodb.backend.d2r.R2DBackendTranslatorImpl;
import com.torodb.backend.meta.TorodbSchema;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.d2r.CollectionData;
import com.torodb.core.d2r.D2RTranslator;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.d2r.DocPartResults;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.d2r.R2DTranslator;
import com.torodb.core.impl.TableRefFactoryImpl;
import com.torodb.core.transaction.BackendException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import com.torodb.d2r.D2RTranslatorStack;
import com.torodb.d2r.IdentifierFactoryImpl;
import com.torodb.d2r.MockRidGenerator;
import com.torodb.d2r.R2DBackedTranslator;
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
import com.torodb.kvdocument.values.heap.LocalDateKVDate;
import com.torodb.kvdocument.values.heap.LocalTimeKVTime;
import com.torodb.kvdocument.values.heap.LongKVInstant;
import com.torodb.kvdocument.values.heap.StringKVString;

public abstract class AbstractBackendTest {
    
    protected static final TableRefFactory tableRefFactory = new TableRefFactoryImpl();
    protected static final MockRidGenerator ridGenerator = new MockRidGenerator();
    protected static final IdentifierFactory identifierFactory = new IdentifierFactoryImpl();
    
    protected DatabaseInterface databaseInterface;
    protected String databaseName;
    protected String databaseSchemaName;
    protected String collectionName;
    protected String collectionIdentifierName;
    protected TableRef rootDocPartTableRef;
    protected String rootDocPartTableName;
    protected ImmutableMap<String, Field<?>> rootDocPartFields;
    protected TableRef subDocPartTableRef;
    protected String subDocPartTableName;
    protected ImmutableMap<String, Field<?>> subDocPartFields;
    protected ImmutableMap<String, Field<?>> newSubDocPartFields;
    protected ImmutableList<ImmutableMap<String, Optional<KVValue<?>>>> rootDocPartValues;
    protected JsonParser parser = new JacksonJsonParser();
    
    protected DataSource dataSource;
    
    @Before
    public void setUp() throws Exception {
        databaseInterface = createDatabaseInterface();
        databaseName = "databaseName";
        databaseSchemaName = "databaseSchemaName";
        collectionName = "collectionName";
        collectionIdentifierName = "collectionIdentifierName";
        rootDocPartTableRef = tableRefFactory.createRoot();
        rootDocPartTableName = "rootDocPartTableName";
        rootDocPartFields = ImmutableMap.<String, Field<?>>builder()
                .put("nullRoot", field("nullRootField", FieldType.NULL))
                .put("booleanRoot", field("booleanRootField", FieldType.BOOLEAN))
                .put("integerRoot", field("integerRootField", FieldType.INTEGER))
                .put("longRoot", field("longRootField", FieldType.LONG))
                .put("doubleRoot", field("doubleRootField", FieldType.DOUBLE))
                .put("stringRoot", field("stringRootField", FieldType.STRING))
                .put("dateRoot", field("dateRootField", FieldType.DATE))
                .put("timeRoot", field("timeRootField", FieldType.TIME))
                .put("mongoObjectIdRoot", field("mongoObjectIdRootField", FieldType.MONGO_OBJECT_ID))
                .put("mongoTimeStampRoot", field("mongoTimeStampRootField", FieldType.MONGO_TIME_STAMP))
                .put("instantRoot", field("instantRootField", FieldType.INSTANT))
                .put("subDocPart", field("subDocPartField", FieldType.CHILD))
                .build();
        subDocPartTableRef = tableRefFactory.createChild(rootDocPartTableRef, "subDocPart");
        subDocPartTableName = "subDocPartTableName";
        subDocPartFields = ImmutableMap.<String, Field<?>>builder()
                .put("nullSub", field("nullSubField", FieldType.NULL))
                .put("booleanSub", field("booleanSubField", FieldType.BOOLEAN))
                .put("integerSub", field("integerSubField", FieldType.INTEGER))
                .put("longSub", field("longSubField", FieldType.LONG))
                .put("doubleSub", field("doubleSubField", FieldType.DOUBLE))
                .put("stringSub", field("stringSubField", FieldType.STRING))
                .put("dateSub", field("dateSubField", FieldType.DATE))
                .put("timeSub", field("timeSubField", FieldType.TIME))
                .put("mongoObjectIdSub", field("mongoObjectIdSubField", FieldType.MONGO_OBJECT_ID))
                .put("mongoTimeStampSub", field("mongoTimeStampSubField", FieldType.MONGO_TIME_STAMP))
                .put("instantSub", field("instantSubField", FieldType.INSTANT))
                .build();
        newSubDocPartFields = ImmutableMap.<String, Field<?>>builder()
                .put("newNullSub", field("newNullSubField", FieldType.NULL))
                .put("newBooleanSub", field("newBooleanSubField", FieldType.BOOLEAN))
                .put("newIntegerSub", field("newIntegerSubField", FieldType.INTEGER))
                .put("newLongSub", field("newLongSubField", FieldType.LONG))
                .put("newDoubleSub", field("newDoubleSubField", FieldType.DOUBLE))
                .put("newStringSub", field("newStringSubField", FieldType.STRING))
                .put("newDateSub", field("newDateSubField", FieldType.DATE))
                .put("newTimeSub", field("newTimeSubField", FieldType.TIME))
                .put("newMongoObjectIdSub", field("newMongoObjectIdSubField", FieldType.MONGO_OBJECT_ID))
                .put("newMongoTimeStampSub", field("newMongoTimeStampSubField", FieldType.MONGO_TIME_STAMP))
                .put("newInstantSub", field("newInstantSubField", FieldType.INSTANT))
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
        dataSource = createDataSource();
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

    protected abstract DataSource createDataSource();

    protected abstract DatabaseInterface createDatabaseInterface();
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected Collection<KVDocument> readDocuments(MetaDatabase metaDatabase, MetaCollection metaCollection,
            DocPartResults<ResultSet> docPartResultSets) throws BackendException, RollbackException {
        R2DTranslator r2dTranslator = new R2DBackedTranslator(new R2DBackendTranslatorImpl(databaseInterface, metaDatabase, metaCollection));
        Collection<KVDocument> readedDocuments = r2dTranslator.translate(docPartResultSets);
        return readedDocuments;
    }

    protected List<Integer> writeCollectionData(DSLContext dsl, CollectionData collectionData) throws BackendException, RollbackException {
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
        return generatedDids;
    }

    protected CollectionData writeDocumentMeta(MutableMetaSnapshot mutableSnapshot, DSLContext dsl, KVDocument document)
            throws Exception {
        CollectionData collectionData = readDataFromDocument(databaseName, collectionName, document, mutableSnapshot);
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
        return collectionData;
    }

    protected CollectionData writeDocumentsMeta(MutableMetaSnapshot mutableSnapshot, DSLContext dsl, List<KVDocument> documents)
            throws Exception {
        CollectionData collectionData = readDataFromDocuments(databaseName, collectionName, documents, mutableSnapshot);
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
        return collectionData;
    }
    
    protected KVDocument parseFromJson(String jsonFileName) throws Exception {
        return parser.createFromResource("docs/" + jsonFileName);
    }
    
    protected CollectionData readDataFromDocument(String database, String collection, KVDocument document, MutableMetaSnapshot mutableSnapshot) throws Exception {
        D2RTranslator translator = new D2RTranslatorStack(tableRefFactory, identifierFactory, ridGenerator, mutableSnapshot, database, collection);
        translator.translate(document);
        return translator.getCollectionDataAccumulator();
    }
    
    protected CollectionData readDataFromDocuments(String database, String collection, List<KVDocument> documents, MutableMetaSnapshot mutableSnapshot) throws Exception {
        D2RTranslator translator = new D2RTranslatorStack(tableRefFactory, identifierFactory, ridGenerator, mutableSnapshot, database, collection);
        for (KVDocument document : documents) {
            translator.translate(document);
        }
        return translator.getCollectionDataAccumulator();
    }
    
    private Field<?> field(String name, FieldType type){
    	return DSL.field(name, databaseInterface.getDataType(type));
    }
}
