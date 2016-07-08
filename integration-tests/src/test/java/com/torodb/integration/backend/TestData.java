package com.torodb.integration.backend;

import java.time.LocalDate;
import java.time.LocalTime;

import com.google.common.collect.ImmutableList;
import com.torodb.backend.SqlInterface;
import com.torodb.core.TableRefFactory;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.ImmutableMetaField;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import com.torodb.core.transaction.metainf.WrapperMutableMetaDocPart;
import com.torodb.kvdocument.values.KVBoolean;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVDouble;
import com.torodb.kvdocument.values.KVInteger;
import com.torodb.kvdocument.values.KVLong;
import com.torodb.kvdocument.values.KVNull;
import com.torodb.kvdocument.values.heap.ByteArrayKVMongoObjectId;
import com.torodb.kvdocument.values.heap.DefaultKVMongoTimestamp;
import com.torodb.kvdocument.values.heap.LocalDateKVDate;
import com.torodb.kvdocument.values.heap.LocalTimeKVTime;
import com.torodb.kvdocument.values.heap.LongKVInstant;
import com.torodb.kvdocument.values.heap.StringKVString;

public class TestData {

    public final ImmutableMetaSnapshot snapshot;
    public final ImmutableMetaDatabase database;
    public final ImmutableMetaCollection collection;
    public final ImmutableMetaDocPart rootDocPart;
    public final ImmutableMetaDocPart subDocPart;
    public final MutableMetaDocPart newSubDocPart;
    public final ImmutableList<KVDocument> documents;
    
    private SqlInterface sqlInterface;
    
    public TestData(TableRefFactory tableRefFactory, SqlInterface sqlInterface){
    	this.sqlInterface = sqlInterface;
        ImmutableMetaSnapshot.Builder metaSnapshotBuilder = new ImmutableMetaSnapshot.Builder();
        ImmutableMetaDatabase.Builder metaDatabaseBuilder = new ImmutableMetaDatabase.Builder("databaseName", "databaseSchemaName");
        ImmutableMetaCollection.Builder metaCollectionBuilder = new ImmutableMetaCollection.Builder("collectionName", "collectionIdentifier");
        ImmutableMetaDocPart.Builder metaRootDocPartBuilder = new ImmutableMetaDocPart.Builder(tableRefFactory.createRoot(), "rootDocPartTableName");
        metaRootDocPartBuilder
                .put(new ImmutableMetaField("nullRoot", "nullRootField", FieldType.NULL))
                .put(new ImmutableMetaField("booleanRoot", "booleanRootField", FieldType.BOOLEAN))
                .put(new ImmutableMetaField("integerRoot", "integerRootField", FieldType.INTEGER))
                .put(new ImmutableMetaField("longRoot", "longRootField", FieldType.LONG))
                .put(new ImmutableMetaField("doubleRoot", "doubleRootField", FieldType.DOUBLE))
                .put(new ImmutableMetaField("stringRoot", "stringRootField", FieldType.STRING))
                .put(new ImmutableMetaField("dateRoot", "dateRootField", FieldType.DATE))
                .put(new ImmutableMetaField("timeRoot", "timeRootField", FieldType.TIME))
                .put(new ImmutableMetaField("mongoObjectIdRoot", "mongoObjectIdRootField", FieldType.MONGO_OBJECT_ID))
                .put(new ImmutableMetaField("mongoTimeStampRoot", "mongoTimeStampRootField", FieldType.MONGO_TIME_STAMP))
                .put(new ImmutableMetaField("instantRoot", "instantRootField", FieldType.INSTANT))
                .put(new ImmutableMetaField("subDocPart", "subDocPartField", FieldType.CHILD));
        rootDocPart = metaRootDocPartBuilder.build();
        metaCollectionBuilder.put(rootDocPart);
        ImmutableMetaDocPart.Builder metaSubDocPartBuilder = new ImmutableMetaDocPart.Builder(tableRefFactory.createChild(rootDocPart.getTableRef(), "subDocPart"), "subDocPartTableName");
        metaSubDocPartBuilder
                .put(new ImmutableMetaField("nullSub", "nullSubField", FieldType.NULL))
                .put(new ImmutableMetaField("booleanSub", "booleanSubField", FieldType.BOOLEAN))
                .put(new ImmutableMetaField("integerSub", "integerSubField", FieldType.INTEGER))
                .put(new ImmutableMetaField("longSub", "longSubField", FieldType.LONG))
                .put(new ImmutableMetaField("doubleSub", "doubleSubField", FieldType.DOUBLE))
                .put(new ImmutableMetaField("stringSub", "stringSubField", FieldType.STRING))
                .put(new ImmutableMetaField("dateSub", "dateSubField", FieldType.DATE))
                .put(new ImmutableMetaField("timeSub", "timeSubField", FieldType.TIME))
                .put(new ImmutableMetaField("mongoObjectIdSub", "mongoObjectIdSubField", FieldType.MONGO_OBJECT_ID))
                .put(new ImmutableMetaField("mongoTimeStampSub", "mongoTimeStampSubField", FieldType.MONGO_TIME_STAMP))
                .put(new ImmutableMetaField("instantSub", "instantSubField", FieldType.INSTANT));
        subDocPart = metaSubDocPartBuilder.build();
        metaCollectionBuilder.put(subDocPart);
        newSubDocPart = new WrapperMutableMetaDocPart(subDocPart, metaDocPart -> {});
        newSubDocPart
                .addMetaField("newNullSub", "newNullSubField", FieldType.NULL);
        newSubDocPart
                .addMetaField("newBooleanSub", "newBooleanSubField", FieldType.BOOLEAN);
        newSubDocPart
                .addMetaField("newIntegerSub", "newIntegerSubField", FieldType.INTEGER);
        newSubDocPart
                .addMetaField("newLongSub", "newLongSubField", FieldType.LONG);
        newSubDocPart
                .addMetaField("newDoubleSub", "newDoubleSubField", FieldType.DOUBLE);
        newSubDocPart
                .addMetaField("newStringSub", "newStringSubField", FieldType.STRING);
        newSubDocPart
                .addMetaField("newDateSub", "newDateSubField", FieldType.DATE);
        newSubDocPart
                .addMetaField("newTimeSub", "newTimeSubField", FieldType.TIME);
        newSubDocPart
                .addMetaField("newMongoObjectIdSub", "newMongoObjectIdSubField", FieldType.MONGO_OBJECT_ID);
        newSubDocPart
                .addMetaField("newMongoTimeStampSub", "newMongoTimeStampSubField", FieldType.MONGO_TIME_STAMP);
        newSubDocPart
                .addMetaField("newInstantSub", "newInstantSubField", FieldType.INSTANT);
        collection = metaCollectionBuilder.build();
        metaDatabaseBuilder.put(collection);
        database = metaDatabaseBuilder.build();
        metaSnapshotBuilder.put(database);
        snapshot = metaSnapshotBuilder.build();
        documents = ImmutableList.<KVDocument>builder()
                .add(new KVDocument.Builder()
                        .putValue("nullRoot", KVNull.getInstance())
                        .putValue("booleanRoot", KVBoolean.TRUE)
                        .putValue("integerRoot", KVInteger.of(1))
                        .putValue("longRoot", KVLong.of(2))
                        .putValue("doubleRoot", KVDouble.of(3.3))
                        .putValue("stringRoot", new StringKVString("Lorem ipsum"))
                        .putValue("dateRoot", new LocalDateKVDate(LocalDate.of(2016, 06, 7)))
                        .putValue("timeRoot", new LocalTimeKVTime(LocalTime.of(17, 29, 00)))
                        .putValue("mongoObjectIdRoot", new ByteArrayKVMongoObjectId(
                                new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}))
                        .putValue("mongoTimeStampRoot", new DefaultKVMongoTimestamp(0, 0))
                        .putValue("instantRoot", new LongKVInstant(0))
                        .putValue("subDocPart", new KVDocument.Builder()
                                        .putValue("nullSub", KVNull.getInstance())
                                        .putValue("booleanSub", KVBoolean.TRUE)
                                        .putValue("integerSub", KVInteger.of(1))
                                        .putValue("longSub", KVLong.of(2))
                                        .putValue("doubleSub", KVDouble.of(3.3))
                                        .putValue("stringSub", new StringKVString("Lorem ipsum"))
                                        .putValue("dateSub", new LocalDateKVDate(LocalDate.of(2016, 06, 7)))
                                        .putValue("timeSub", new LocalTimeKVTime(LocalTime.of(17, 29, 00)))
                                        .putValue("mongoObjectIdSub", new ByteArrayKVMongoObjectId(
                                                new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}))
                                        .putValue("mongoTimeStampSub", new DefaultKVMongoTimestamp(0, 0))
                                        .putValue("instantSub", new LongKVInstant(0))
                                        .build())
                        .build())
                .add(new KVDocument.Builder()
                        .build())
                .build();
    }
    
    public ImmutableList<KVDocument> getMoreDocuments(){
    	return ImmutableList.<KVDocument>builder()
        .add(new KVDocument.Builder()
                .putValue("nullRoot", KVNull.getInstance())
                .putValue("booleanRoot", KVBoolean.FALSE)
                .putValue("subDocPart", new KVDocument.Builder()
                        .putValue("nullSub", KVNull.getInstance())
                        .putValue("booleanSub", KVBoolean.FALSE)
                        .build())
                .build())
        .build();
    }
    
}