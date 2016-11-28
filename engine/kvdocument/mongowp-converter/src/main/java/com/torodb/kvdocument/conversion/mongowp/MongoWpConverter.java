/*
 * ToroDB
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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
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
import com.torodb.kvdocument.types.ArrayType;
import com.torodb.kvdocument.types.BinaryType;
import com.torodb.kvdocument.types.BooleanType;
import com.torodb.kvdocument.types.DocumentType;
import com.torodb.kvdocument.types.DoubleType;
import com.torodb.kvdocument.types.GenericType;
import com.torodb.kvdocument.types.InstantType;
import com.torodb.kvdocument.types.IntegerType;
import com.torodb.kvdocument.types.KvType;
import com.torodb.kvdocument.types.LongType;
import com.torodb.kvdocument.types.MongoObjectIdType;
import com.torodb.kvdocument.types.MongoTimestampType;
import com.torodb.kvdocument.types.NullType;
import com.torodb.kvdocument.types.StringType;
import com.torodb.kvdocument.values.KvArray;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.kvdocument.values.heap.ListKvArray;
import com.torodb.kvdocument.values.heap.MapKvDocument;

import java.util.LinkedHashMap;

import javax.annotation.Nonnull;

/**
 *
 */
public class MongoWpConverter {

  public static final Function<BsonValue<?>, KvValue<?>> FROM_BSON = new FromBsonFunction();

  private MongoWpConverter() {
  }

  public static KvType translate(BsonType bsonType) throws UnsupportedBsonTypeException {
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
      case DEPRECATED:
        throw new UnsupportedBsonTypeException(bsonType);
      default:
        throw new AssertionError("It seems that " + bsonType + " has "
            + "been added to " + BsonType.class.getCanonicalName()
            + " but it is not defined how to translate it to "
            + KvType.class.getCanonicalName());
    }
  }

  public static KvValue<?> translate(BsonValue<?> bson) {
    return bson.accept(FromBsonValueTranslator.getInstance(), null);
  }

  public static BsonValue<?> translate(KvValue<?> kvValue) {
    return kvValue.accept(ToBsonValueTranslator.getInstance(), null);
  }

  public static KvDocument toEagerDocument(BsonDocument doc) {
    LinkedHashMap<String, KvValue<?>> map = new LinkedHashMap<>(doc.size());
    for (Entry<?> entry : doc) {
      map.put(entry.getKey(), MongoWpConverter.translate(entry.getValue()));
    }
    return new MapKvDocument(map);
  }

  public static KvArray toEagerArray(BsonArray array) {
    return new ListKvArray(Lists.newArrayList(Iterators.transform(array.iterator(), FROM_BSON)));
  }

  public static class FromBsonFunction implements Function<BsonValue<?>, KvValue<?>> {

    @Override
    public KvValue<?> apply(@Nonnull BsonValue<?> input) {
      return translate(input);
    }

  }

}
