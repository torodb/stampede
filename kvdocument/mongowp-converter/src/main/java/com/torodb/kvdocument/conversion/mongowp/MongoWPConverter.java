/*
 * This file is part of ToroDB.
 *
 * ToroDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ToroDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with mongowp-converter. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.kvdocument.conversion.mongowp;

import com.eightkdata.mongowp.bson.BsonArray;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonDocument.Entry;
import com.eightkdata.mongowp.bson.BsonType;
import com.eightkdata.mongowp.bson.BsonValue;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.torodb.kvdocument.types.*;
import com.torodb.kvdocument.values.KVArray;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.kvdocument.values.heap.ListKVArray;
import com.torodb.kvdocument.values.heap.MapKVDocument;
import java.util.LinkedHashMap;
import javax.annotation.Nonnull;

/**
 *
 */
public class MongoWPConverter {

    public static final Function<BsonValue<?>, KVValue<?>> FROM_BSON = new FromBsonFunction();

    private MongoWPConverter() {
    }

    public static KVType translate(BsonType bsonType) throws UnsupportedBsonTypeException {
        switch (bsonType) {
            case ARRAY:
                return new ArrayType(GenericType.INSTANCE);
            case BINARY:
                return BinaryType.INSTANCE;
            case BOOLEAN:
                return BooleanType.INSTANCE;
            case DATETIME:
                return InstantType.INSTANCE;
            case DOCUMENT:
                return DocumentType.INSTANCE;
            case DOUBLE:
                return DoubleType.INSTANCE;
            case INT32:
                return IntegerType.INSTANCE;
            case INT64:
                return LongType.INSTANCE;
            case NULL:
                return NullType.INSTANCE;
            case OBJECT_ID:
                return MongoObjectIdType.INSTANCE;
            case STRING:
                return StringType.INSTANCE;
            case TIMESTAMP:
                return MongoTimestampType.INSTANCE;
            case REGEX:
            case UNDEFINED:
            case JAVA_SCRIPT:
            case JAVA_SCRIPT_WITH_SCOPE:
            case MAX:
            case MIN:
            case DB_POINTER:
            case DEPRECTED:
                throw new UnsupportedBsonTypeException(bsonType);
            default:
                throw new AssertionError("It seems that " + bsonType + " has "
                        + "been added to " + BsonType.class.getCanonicalName()
                        + " but it is not defined how to translate it to "
                        + KVType.class.getCanonicalName());
        }
    }

    public static KVValue<?> translate(BsonValue<?> bson) {
        return bson.accept(FromBsonValueTranslator.getInstance(), null);
    }

    public static BsonValue<?> translate(KVValue<?> kvValue) {
        return kvValue.accept(ToBsonValueTranslator.getInstance(), null);
    }

    public static KVDocument toEagerDocument(BsonDocument doc) {
        LinkedHashMap<String, KVValue<?>> map = new LinkedHashMap<>(doc.size());
        for (Entry<?> entry : doc) {
            map.put(entry.getKey(), MongoWPConverter.translate(entry.getValue()));
        }
        return new MapKVDocument(map);
    }

    public static KVArray toEagerArray(BsonArray array) {
        return new ListKVArray(Lists.newArrayList(Iterators.transform(array.iterator(), FROM_BSON)));
    }

    public static class FromBsonFunction implements Function<BsonValue<?>, KVValue<?>> {

        @Override
        public KVValue<?> apply(@Nonnull BsonValue<?> input) {
            return translate(input);
        }

    }

}
