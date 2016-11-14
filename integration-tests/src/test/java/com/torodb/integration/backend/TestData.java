/*
 * ToroDB - ToroDB-poc: Integration Tests
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.torodb.integration.backend;

import java.time.LocalDate;
import java.time.LocalTime;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSource;
import com.torodb.backend.SqlInterface;
import com.torodb.core.TableRefFactory;
import com.torodb.core.transaction.metainf.FieldIndexOrdering;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.ImmutableMetaIdentifiedDocPartIndex;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPartIndexColumn;
import com.torodb.core.transaction.metainf.ImmutableMetaField;
import com.torodb.core.transaction.metainf.ImmutableMetaIndex;
import com.torodb.core.transaction.metainf.ImmutableMetaIndexField;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import com.torodb.core.transaction.metainf.WrapperMutableMetaDocPart;
import com.torodb.kvdocument.values.KVBinary.KVBinarySubtype;
import com.torodb.kvdocument.values.KVBoolean;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVDouble;
import com.torodb.kvdocument.values.KVInteger;
import com.torodb.kvdocument.values.KVLong;
import com.torodb.kvdocument.values.KVNull;
import com.torodb.kvdocument.values.heap.ByteArrayKVMongoObjectId;
import com.torodb.kvdocument.values.heap.ByteSourceKVBinary;
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
    public final ImmutableMetaIndex index;
    
    public TestData(TableRefFactory tableRefFactory, SqlInterface sqlInterface){
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
                .put(new ImmutableMetaField("binaryRoot", "binaryRootField", FieldType.BINARY))
                .put(new ImmutableMetaField("mongoObjectIdRoot", "mongoObjectIdRootField", FieldType.MONGO_OBJECT_ID))
                .put(new ImmutableMetaField("mongoTimeStampRoot", "mongoTimeStampRootField", FieldType.MONGO_TIME_STAMP))
                .put(new ImmutableMetaField("instantRoot", "instantRootField", FieldType.INSTANT))
                .put(new ImmutableMetaField("subDocPart", "subDocPartField", FieldType.CHILD))
                .put(new ImmutableMetaIdentifiedDocPartIndex.Builder("rootDocPartIndex", false)
                        .add(new ImmutableMetaDocPartIndexColumn(0, "integerRootField", FieldIndexOrdering.ASC)).build());
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
                .put(new ImmutableMetaField("binarySub", "binarySubField", FieldType.BINARY))
                .put(new ImmutableMetaField("mongoObjectIdSub", "mongoObjectIdSubField", FieldType.MONGO_OBJECT_ID))
                .put(new ImmutableMetaField("mongoTimeStampSub", "mongoTimeStampSubField", FieldType.MONGO_TIME_STAMP))
                .put(new ImmutableMetaField("instantSub", "instantSubField", FieldType.INSTANT))
                .put(new ImmutableMetaIdentifiedDocPartIndex.Builder("subDocPartIndex", false)
                        .add(new ImmutableMetaDocPartIndexColumn(0, "longSubField", FieldIndexOrdering.ASC)).build());
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
            .addMetaField("newBinarySub", "newBinarySubField", FieldType.BINARY);
        newSubDocPart
                .addMetaField("newMongoObjectIdSub", "newMongoObjectIdSubField", FieldType.MONGO_OBJECT_ID);
        newSubDocPart
                .addMetaField("newMongoTimeStampSub", "newMongoTimeStampSubField", FieldType.MONGO_TIME_STAMP);
        newSubDocPart
                .addMetaField("newInstantSub", "newInstantSubField", FieldType.INSTANT);
        ImmutableMetaIndex.Builder metaIndexBuilder = new ImmutableMetaIndex.Builder("index", false);
        metaIndexBuilder
                .add(new ImmutableMetaIndexField(0, rootDocPart.getTableRef(), "integerRoot", FieldIndexOrdering.ASC))
                .add(new ImmutableMetaIndexField(1, subDocPart.getTableRef(), "integerSub", FieldIndexOrdering.ASC))
                .add(new ImmutableMetaIndexField(2, subDocPart.getTableRef(), "longSub", FieldIndexOrdering.ASC));
        index = metaIndexBuilder.build();
        metaCollectionBuilder.put(index);
        metaCollectionBuilder.put(subDocPart);
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
                        .putValue("binaryRoot", new ByteSourceKVBinary(KVBinarySubtype.MONGO_GENERIC, (byte) 0, ByteSource.wrap(new byte[] { 1, 2, 3, 4 })))
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
                                        .putValue("binarySub", new ByteSourceKVBinary(KVBinarySubtype.MONGO_GENERIC, (byte) 0, ByteSource.wrap(new byte[] { 1, 2, 3, 4 })))
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
