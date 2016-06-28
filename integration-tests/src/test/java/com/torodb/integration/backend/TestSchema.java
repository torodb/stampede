package com.torodb.integration.backend;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Optional;

import org.jooq.Field;
import org.jooq.impl.DSL;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.torodb.backend.SqlInterface;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.transaction.metainf.FieldType;
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

    public final String databaseName;
    public final String databaseSchemaName;
    public final String collectionName;
    public final String collectionIdentifierName;
    public final TableRef rootDocPartTableRef;
    public final String rootDocPartTableName;
    public final ImmutableMap<String, Field<?>> rootDocPartFields;
    public final TableRef subDocPartTableRef;
    public final String subDocPartTableName;
    public final ImmutableMap<String, Field<?>> subDocPartFields;
    public final ImmutableMap<String, Field<?>> newSubDocPartFields;
    public final ImmutableList<ImmutableMap<String, Optional<KVValue<?>>>> rootDocPartValues;
    public final ImmutableList<ImmutableMap<String, Optional<KVValue<?>>>> subDocPartValues;
    
    private SqlInterface sqlInterface;
    
    public TestSchema(TableRefFactory tableRefFactory, SqlInterface sqlInterface){
    	this.sqlInterface = sqlInterface;
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
    
    private Field<?> field(String name, FieldType type){
    	return DSL.field(name, sqlInterface.getDataType(type));
    }

}
