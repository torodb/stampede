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

import static com.eightkdata.mongowp.bson.BinarySubtype.FUNCTION;
import static com.eightkdata.mongowp.bson.BinarySubtype.GENERIC;
import static com.eightkdata.mongowp.bson.BinarySubtype.MD5;
import static com.eightkdata.mongowp.bson.BinarySubtype.OLD_BINARY;
import static com.eightkdata.mongowp.bson.BinarySubtype.OLD_UUID;
import static com.eightkdata.mongowp.bson.BinarySubtype.USER_DEFINED;
import static com.eightkdata.mongowp.bson.BinarySubtype.UUID;

import com.eightkdata.mongowp.bson.BsonArray;
import com.eightkdata.mongowp.bson.BsonBinary;
import com.eightkdata.mongowp.bson.BsonBoolean;
import com.eightkdata.mongowp.bson.BsonDateTime;
import com.eightkdata.mongowp.bson.BsonDbPointer;
import com.eightkdata.mongowp.bson.BsonDeprecated;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonDouble;
import com.eightkdata.mongowp.bson.BsonInt32;
import com.eightkdata.mongowp.bson.BsonInt64;
import com.eightkdata.mongowp.bson.BsonJavaScript;
import com.eightkdata.mongowp.bson.BsonJavaScriptWithScope;
import com.eightkdata.mongowp.bson.BsonMax;
import com.eightkdata.mongowp.bson.BsonMin;
import com.eightkdata.mongowp.bson.BsonNull;
import com.eightkdata.mongowp.bson.BsonObjectId;
import com.eightkdata.mongowp.bson.BsonRegex;
import com.eightkdata.mongowp.bson.BsonString;
import com.eightkdata.mongowp.bson.BsonTimestamp;
import com.eightkdata.mongowp.bson.BsonUndefined;
import com.eightkdata.mongowp.bson.BsonValue;
import com.eightkdata.mongowp.bson.BsonValueVisitor;
import com.eightkdata.mongowp.bson.impl.InstantBsonDateTime;
import com.torodb.kvdocument.conversion.mongowp.values.BsonKvString;
import com.torodb.kvdocument.values.KvBinary.KvBinarySubtype;
import com.torodb.kvdocument.values.KvBoolean;
import com.torodb.kvdocument.values.KvDouble;
import com.torodb.kvdocument.values.KvInteger;
import com.torodb.kvdocument.values.KvLong;
import com.torodb.kvdocument.values.KvNull;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.kvdocument.values.heap.ByteArrayKvMongoObjectId;
import com.torodb.kvdocument.values.heap.ByteSourceKvBinary;
import com.torodb.kvdocument.values.heap.DefaultKvMongoTimestamp;
import com.torodb.kvdocument.values.heap.InstantKvInstant;
import com.torodb.kvdocument.values.heap.LongKvInstant;

import java.util.function.Function;

/**
 *
 */
public class FromBsonValueTranslator implements BsonValueVisitor<KvValue<?>, Void>,
    Function<BsonValue<?>, KvValue<?>> {

  private FromBsonValueTranslator() {
  }

  public static FromBsonValueTranslator getInstance() {
    return FromBsonValueTranslatorHolder.INSTANCE;
  }

  @Override
  public KvValue<?> apply(BsonValue<?> bsonValue) {
    return bsonValue.accept(this, null);
  }

  @Override
  public KvValue<?> visit(BsonArray value, Void arg) {
    return MongoWpConverter.toEagerArray(value);
  }

  @Override
  public KvValue<?> visit(BsonBinary value, Void arg) {
    KvBinarySubtype subtype;
    switch (value.getSubtype()) {
      case FUNCTION:
        subtype = KvBinarySubtype.MONGO_FUNCTION;
        break;
      case GENERIC:
        subtype = KvBinarySubtype.MONGO_GENERIC;
        break;
      case MD5:
        subtype = KvBinarySubtype.MONGO_MD5;
        break;
      case OLD_BINARY:
        subtype = KvBinarySubtype.MONGO_OLD_BINARY;
        break;
      case OLD_UUID:
        subtype = KvBinarySubtype.MONGO_OLD_UUID;
        break;
      case USER_DEFINED:
        subtype = KvBinarySubtype.MONGO_USER_DEFINED;
        break;
      case UUID:
        subtype = KvBinarySubtype.MONGO_UUID;
        break;
      default:
        subtype = KvBinarySubtype.UNDEFINED;
        break;
    }
    return new ByteSourceKvBinary(subtype, value.getNumericSubType(), value.getByteSource()
        .getDelegate());
  }

  @Override
  public KvValue<?> visit(BsonDbPointer value, Void arg) {
    throw new UnsupportedBsonTypeException(value.getType());
  }

  @Override
  public KvValue<?> visit(BsonDateTime value, Void arg) {
    if (value instanceof InstantBsonDateTime) {
      return new InstantKvInstant(value.getValue());
    }
    return new LongKvInstant(value.getMillisFromUnix());
  }

  @Override
  public KvValue<?> visit(BsonDocument value, Void arg) {
    return MongoWpConverter.toEagerDocument(value);
  }

  @Override
  public KvValue<?> visit(BsonDouble value, Void arg) {
    return KvDouble.of(value.doubleValue());
  }

  @Override
  public KvValue<?> visit(BsonInt32 value, Void arg) {
    return KvInteger.of(value.intValue());
  }

  @Override
  public KvValue<?> visit(BsonInt64 value, Void arg) {
    return KvLong.of(value.longValue());
  }

  @Override
  public KvValue<?> visit(BsonBoolean value, Void arg) {
    if (value.getPrimitiveValue()) {
      return KvBoolean.TRUE;
    }
    return KvBoolean.FALSE;
  }

  @Override
  public KvValue<?> visit(BsonJavaScript value, Void arg) {
    throw new UnsupportedBsonTypeException(value.getType());
  }

  @Override
  public KvValue<?> visit(BsonJavaScriptWithScope value, Void arg) {
    throw new UnsupportedBsonTypeException(value.getType());
  }

  @Override
  public KvValue<?> visit(BsonMax value, Void arg) {
    throw new UnsupportedBsonTypeException(value.getType());
  }

  @Override
  public KvValue<?> visit(BsonMin value, Void arg) {
    throw new UnsupportedBsonTypeException(value.getType());
  }

  @Override
  public KvValue<?> visit(BsonNull value, Void arg) {
    return KvNull.getInstance();
  }

  @Override
  public KvValue<?> visit(BsonObjectId value, Void arg) {
    return new ByteArrayKvMongoObjectId(value.toByteArray());
  }

  @Override
  public KvValue<?> visit(BsonRegex value, Void arg) {
    throw new UnsupportedBsonTypeException(value.getType());
  }

  @Override
  public KvValue<?> visit(BsonString value, Void arg) {
    return new BsonKvString(value);
  }

  @Override
  public KvValue<?> visit(BsonUndefined value, Void arg) {
    return KvNull.getInstance();
  }

  @Override
  public KvValue<?> visit(BsonTimestamp value, Void arg) {
    return new DefaultKvMongoTimestamp(value.getSecondsSinceEpoch(), value.getOrdinal());
  }

  @Override
  public KvValue<?> visit(BsonDeprecated value, Void arg) {
    throw new UnsupportedBsonTypeException(value.getType());
  }

  private static class FromBsonValueTranslatorHolder {

    private static final FromBsonValueTranslator INSTANCE = new FromBsonValueTranslator();
  }

  //@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD")
  private Object readResolve() {
    return FromBsonValueTranslator.getInstance();
  }
}
