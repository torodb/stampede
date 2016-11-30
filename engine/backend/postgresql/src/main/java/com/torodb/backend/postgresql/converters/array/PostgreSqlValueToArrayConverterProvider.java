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

package com.torodb.backend.postgresql.converters.array;

import com.google.common.collect.Maps;
import com.torodb.backend.converters.array.ArrayConverter;
import com.torodb.backend.converters.array.BinaryToArrayConverter;
import com.torodb.backend.converters.array.BooleanToArrayConverter;
import com.torodb.backend.converters.array.DateToArrayConverter;
import com.torodb.backend.converters.array.DoubleToArrayConverter;
import com.torodb.backend.converters.array.InstantToArrayConverter;
import com.torodb.backend.converters.array.IntegerToArrayConverter;
import com.torodb.backend.converters.array.LongToArrayConverter;
import com.torodb.backend.converters.array.MongoObjectIdToArrayConverter;
import com.torodb.backend.converters.array.MongoTimestampToArrayConverter;
import com.torodb.backend.converters.array.NullToArrayConverter;
import com.torodb.backend.converters.array.StringToArrayConverter;
import com.torodb.backend.converters.array.TimeToArrayConverter;
import com.torodb.backend.converters.array.ValueToArrayConverterProvider;
import com.torodb.core.exceptions.SystemException;
import com.torodb.kvdocument.types.ArrayType;
import com.torodb.kvdocument.types.BinaryType;
import com.torodb.kvdocument.types.BooleanType;
import com.torodb.kvdocument.types.DateType;
import com.torodb.kvdocument.types.DoubleType;
import com.torodb.kvdocument.types.InstantType;
import com.torodb.kvdocument.types.IntegerType;
import com.torodb.kvdocument.types.KvType;
import com.torodb.kvdocument.types.LongType;
import com.torodb.kvdocument.types.MongoObjectIdType;
import com.torodb.kvdocument.types.MongoTimestampType;
import com.torodb.kvdocument.types.NullType;
import com.torodb.kvdocument.types.StringType;
import com.torodb.kvdocument.types.TimeType;
import com.torodb.kvdocument.values.KvArray;
import com.torodb.kvdocument.values.KvBinary;
import com.torodb.kvdocument.values.KvBoolean;
import com.torodb.kvdocument.values.KvDate;
import com.torodb.kvdocument.values.KvInstant;
import com.torodb.kvdocument.values.KvInteger;
import com.torodb.kvdocument.values.KvLong;
import com.torodb.kvdocument.values.KvMongoObjectId;
import com.torodb.kvdocument.values.KvNull;
import com.torodb.kvdocument.values.KvString;
import com.torodb.kvdocument.values.KvTime;

import java.util.Map;

import javax.annotation.Nonnull;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;

public class PostgreSqlValueToArrayConverterProvider implements ValueToArrayConverterProvider {

  private static final long serialVersionUID = 1L;

  private final Map<Class<? extends KvType>, ArrayConverter<?, ?>> converters;
  private final ArrayConverter<JsonArray, KvArray> arrayConverter;
  private final ArrayConverter<JsonValue, KvBoolean> booleanConverter;
  private final ArrayConverter<JsonString, KvDate> dateConverter;
  private final ArrayConverter<JsonString, KvInstant> dateTimeConverter;
  private final DoubleToArrayConverter doubleConverter;
  private final ArrayConverter<JsonNumber, KvInteger> integerConverter;
  private final ArrayConverter<JsonNumber, KvLong> longConverter;
  private final ArrayConverter<JsonValue, KvNull> nullConverter;
  private final ArrayConverter<JsonString, KvString> stringConverter;
  private final ArrayConverter<JsonString, KvTime> timeConverter;
  private final ArrayConverter<JsonString, KvMongoObjectId> mongoObjectIdConverter;
  private final MongoTimestampToArrayConverter mongoTimestampConverter;
  private final ArrayConverter<JsonString, KvBinary> binaryConverter;

  private PostgreSqlValueToArrayConverterProvider() {
    arrayConverter = new ArrayToArrayConverter(this);
    booleanConverter = new BooleanToArrayConverter();
    dateConverter = new DateToArrayConverter();
    dateTimeConverter = new InstantToArrayConverter();
    doubleConverter = new DoubleToArrayConverter();
    integerConverter = new IntegerToArrayConverter();
    longConverter = new LongToArrayConverter();
    nullConverter = new NullToArrayConverter();
    stringConverter = new StringToArrayConverter();
    timeConverter = new TimeToArrayConverter();
    mongoObjectIdConverter = new MongoObjectIdToArrayConverter();
    mongoTimestampConverter = new MongoTimestampToArrayConverter();
    binaryConverter = new BinaryToArrayConverter();

    converters = Maps.newHashMap();
    converters.put(ArrayType.class, arrayConverter);
    converters.put(BooleanType.class, booleanConverter);
    converters.put(DateType.class, dateConverter);
    converters.put(InstantType.class, dateTimeConverter);
    converters.put(DoubleType.class, doubleConverter);
    converters.put(IntegerType.class, integerConverter);
    converters.put(LongType.class, longConverter);
    converters.put(NullType.class, nullConverter);
    converters.put(StringType.class, stringConverter);
    converters.put(TimeType.class, timeConverter);
    converters.put(MongoObjectIdType.class, mongoObjectIdConverter);
    converters.put(MongoTimestampType.class, mongoTimestampConverter);
    converters.put(BinaryType.class, binaryConverter);

  }

  public static PostgreSqlValueToArrayConverterProvider getInstance() {
    return ToArrayConverterHolder.INSTANCE;
  }

  @Override
  @Nonnull
  public ArrayConverter<?, ?> getConverter(KvType valueType) {
    ArrayConverter<?, ?> converter = converters.get(valueType.getClass());
    if (converter == null) {
      throw new AssertionError("There is no converter that converts "
          + "elements of type " + valueType);
    }
    return converter;
  }

  @Override
  @Nonnull
  public ArrayConverter<?, ?> fromJsonValue(JsonValue jsonValue) {
    switch (jsonValue.getValueType()) {
      case ARRAY:
        assert jsonValue instanceof JsonArray;
        return arrayConverter;
      case TRUE:
      case FALSE:
        return booleanConverter;
      case NULL:
        return nullConverter;
      case NUMBER:
        assert jsonValue instanceof JsonNumber;
        JsonNumber number = (JsonNumber) jsonValue;
        if (number.isIntegral()) {
          try {
            long l = number.longValueExact();
            if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
              return longConverter;
            }
            return integerConverter;
          } catch (ArithmeticException ex) {
            throw new SystemException(
                "Unexpected integral value. " + number + " is "
                + "bigger than long values"
            );
          }
        }
        return doubleConverter;
      case STRING:
        assert jsonValue instanceof JsonString;
        return stringConverter;
      case OBJECT: {
        JsonObject asObject = ((JsonObject) jsonValue);
        if (mongoTimestampConverter.isValid(asObject)) {
          return mongoTimestampConverter;
        }
        throw new IllegalArgumentException("Te recived JsonObject " + jsonValue
            + " was not recognized as a valid KVValue codification");
      }
      default:
        throw new IllegalArgumentException("Instances of '"
            + jsonValue.getClass() + "' like '" + jsonValue
            + "' are not supported");
    }
  }

  public ArrayConverter<JsonArray, KvArray> getArrayConverter() {
    return arrayConverter;
  }

  public ArrayConverter<JsonValue, KvBoolean> getBooleanConverter() {
    return booleanConverter;
  }

  public ArrayConverter<JsonString, KvDate> getDateConverter() {
    return dateConverter;
  }

  public ArrayConverter<JsonString, KvInstant> getInstantConverter() {
    return dateTimeConverter;
  }

  public DoubleToArrayConverter getDoubleConverter() {
    return doubleConverter;
  }

  public ArrayConverter<JsonNumber, KvInteger> getIntegerConverter() {
    return integerConverter;
  }

  public ArrayConverter<JsonNumber, KvLong> getLongConverter() {
    return longConverter;
  }

  public ArrayConverter<JsonValue, KvNull> getNullConverter() {
    return nullConverter;
  }

  public ArrayConverter<JsonString, KvString> getStringConverter() {
    return stringConverter;
  }

  public ArrayConverter<JsonString, KvTime> getTimeConverter() {
    return timeConverter;
  }

  public ArrayConverter<JsonString, KvMongoObjectId> getMongoObjectIdConverter() {
    return mongoObjectIdConverter;
  }

  public MongoTimestampToArrayConverter getMongoTimestampConverter() {
    return mongoTimestampConverter;
  }

  public ArrayConverter<JsonString, KvBinary> getBinaryConverter() {
    return binaryConverter;
  }

  private static class ToArrayConverterHolder {

    private static final PostgreSqlValueToArrayConverterProvider INSTANCE =
        new PostgreSqlValueToArrayConverterProvider();
  }

  //@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD")
  private Object readResolve() {
    return PostgreSqlValueToArrayConverterProvider.getInstance();
  }

}
