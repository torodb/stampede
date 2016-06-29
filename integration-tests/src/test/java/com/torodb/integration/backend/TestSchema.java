package com.torodb.integration.backend;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Optional;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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

public class TestSchema {

    public final ImmutableMetaSnapshot snapshot;
    public final ImmutableMetaDatabase database;
    public final ImmutableMetaCollection collection;
    public final ImmutableMetaDocPart rootDocPart;
    public final ImmutableMetaDocPart subDocPart;
    public final MutableMetaDocPart newSubDocPart;
    public final ImmutableList<ImmutableMap<String, Optional<KVValue<?>>>> rootDocPartValues;
    public final ImmutableList<ImmutableMap<String, Optional<KVValue<?>>>> subDocPartValues;
    
    private SqlInterface sqlInterface;
    
    public TestSchema(TableRefFactory tableRefFactory, SqlInterface sqlInterface){
    	this.sqlInterface = sqlInterface;
        ImmutableMetaSnapshot.Builder metaSnapshotBuilder = new ImmutableMetaSnapshot.Builder();
        ImmutableMetaDatabase.Builder metaDatabaseBuilder = new ImmutableMetaDatabase.Builder("databaseName", "databaseSchemaName");
        ImmutableMetaCollection.Builder metaCollectionBuilder = new ImmutableMetaCollection.Builder("collectionName", "collectionIdentifier");
        ImmutableMetaDocPart.Builder metaRootDocPartBuilder = new ImmutableMetaDocPart.Builder(tableRefFactory.createRoot(), "rootDocPartTableName");
        metaRootDocPartBuilder
                .add(new ImmutableMetaField("nullRoot", "nullRootField", FieldType.NULL))
                .add(new ImmutableMetaField("booleanRoot", "booleanRootField", FieldType.BOOLEAN))
                .add(new ImmutableMetaField("integerRoot", "integerRootField", FieldType.INTEGER))
                .add(new ImmutableMetaField("longRoot", "longRootField", FieldType.LONG))
                .add(new ImmutableMetaField("doubleRoot", "doubleRootField", FieldType.DOUBLE))
                .add(new ImmutableMetaField("stringRoot", "stringRootField", FieldType.STRING))
                .add(new ImmutableMetaField("dateRoot", "dateRootField", FieldType.DATE))
                .add(new ImmutableMetaField("timeRoot", "timeRootField", FieldType.TIME))
                .add(new ImmutableMetaField("mongoObjectIdRoot", "mongoObjectIdRootField", FieldType.MONGO_OBJECT_ID))
                .add(new ImmutableMetaField("mongoTimeStampRoot", "mongoTimeStampRootField", FieldType.MONGO_TIME_STAMP))
                .add(new ImmutableMetaField("instantRoot", "instantRootField", FieldType.INSTANT))
                .add(new ImmutableMetaField("subDocPart", "subDocPartField", FieldType.CHILD));
        rootDocPart = metaRootDocPartBuilder.build();
        metaCollectionBuilder.add(rootDocPart);
        ImmutableMetaDocPart.Builder metaSubDocPartBuilder = new ImmutableMetaDocPart.Builder(tableRefFactory.createChild(rootDocPart.getTableRef(), "subDocPart"), "subDocPartTableName");
        metaSubDocPartBuilder
                .add(new ImmutableMetaField("nullSub", "nullSubField", FieldType.NULL))
                .add(new ImmutableMetaField("booleanSub", "booleanSubField", FieldType.BOOLEAN))
                .add(new ImmutableMetaField("integerSub", "integerSubField", FieldType.INTEGER))
                .add(new ImmutableMetaField("longSub", "longSubField", FieldType.LONG))
                .add(new ImmutableMetaField("doubleSub", "doubleSubField", FieldType.DOUBLE))
                .add(new ImmutableMetaField("stringSub", "stringSubField", FieldType.STRING))
                .add(new ImmutableMetaField("dateSub", "dateSubField", FieldType.DATE))
                .add(new ImmutableMetaField("timeSub", "timeSubField", FieldType.TIME))
                .add(new ImmutableMetaField("mongoObjectIdSub", "mongoObjectIdSubField", FieldType.MONGO_OBJECT_ID))
                .add(new ImmutableMetaField("mongoTimeStampSub", "mongoTimeStampSubField", FieldType.MONGO_TIME_STAMP))
                .add(new ImmutableMetaField("instantSub", "instantSubField", FieldType.INSTANT));
        subDocPart = metaSubDocPartBuilder.build();
        metaCollectionBuilder.add(subDocPart);
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
        metaDatabaseBuilder.add(collection);
        database = metaDatabaseBuilder.build();
        metaSnapshotBuilder.add(database);
        snapshot = metaSnapshotBuilder.build();
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
        subDocPartValues = ImmutableList.<ImmutableMap<String, Optional<KVValue<?>>>>builder()
                .add(ImmutableMap.<String, Optional<KVValue<?>>>builder()
                        .put("nullSub", Optional.of(KVNull.getInstance()))
                        .put("booleanSub", Optional.of(KVBoolean.TRUE))
                        .put("integerSub", Optional.of(KVInteger.of(1)))
                        .put("longSub", Optional.of(KVLong.of(2)))
                        .put("doubleSub", Optional.of(KVDouble.of(3.3)))
                        .put("stringSub", Optional.of(new StringKVString("Lorem ipsum")))
                        .put("dateSub", Optional.of(new LocalDateKVDate(LocalDate.of(2016, 06, 7))))
                        .put("timeSub", Optional.of(new LocalTimeKVTime(LocalTime.of(17, 29, 00))))
                        .put("mongoObjectIdSub", Optional.of(new ByteArrayKVMongoObjectId(
                                new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})))
                        .put("mongoTimeStampSub", Optional.of(new DefaultKVMongoTimestamp(0, 0)))
                        .put("instantSub", Optional.of(new LongKVInstant(0)))
                        .build())
               .build();
                
    }
    
    public ImmutableList<ImmutableMap<String, Optional<KVValue<?>>>> getMoreRootDocPartValues(){
    	return ImmutableList.<ImmutableMap<String, Optional<KVValue<?>>>>builder()
        .add(ImmutableMap.<String, Optional<KVValue<?>>>builder()
                .put("nullRoot", Optional.of(KVNull.getInstance()))
                .put("booleanRoot", Optional.of(KVBoolean.FALSE))
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
    }
    
    public ImmutableList<ImmutableMap<String, Optional<KVValue<?>>>> getMoreSubDocPartValues(){
    	return ImmutableList.<ImmutableMap<String, Optional<KVValue<?>>>>builder()
                .add(ImmutableMap.<String, Optional<KVValue<?>>>builder()
                        .put("nullSub", Optional.of(KVNull.getInstance()))
                        .put("booleanSub", Optional.of(KVBoolean.FALSE))
                        .put("integerSub", Optional.empty())
                        .put("longSub", Optional.empty())
                        .put("doubleSub", Optional.empty())
                        .put("stringSub", Optional.empty())
                        .put("dateSub", Optional.empty())
                        .put("timeSub", Optional.empty())
                        .put("mongoObjectIdSub", Optional.empty())
                        .put("mongoTimeStampSub", Optional.empty())
                        .put("instantSub", Optional.empty())
                        .build())
               .build();
    }
    
}
