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

import static com.eightkdata.mongowp.bson.utils.DefaultBsonValues.newArray;
import static com.eightkdata.mongowp.bson.utils.DefaultBsonValues.newBoolean;
import static com.eightkdata.mongowp.bson.utils.DefaultBsonValues.newDecimal128;
import static com.eightkdata.mongowp.bson.utils.DefaultBsonValues.newDocument;
import static com.eightkdata.mongowp.bson.utils.DefaultBsonValues.newDouble;
import static com.eightkdata.mongowp.bson.utils.DefaultBsonValues.newInt;
import static com.eightkdata.mongowp.bson.utils.DefaultBsonValues.newLong;
import static com.eightkdata.mongowp.bson.utils.DefaultBsonValues.newString;

import com.eightkdata.mongowp.bson.*;
import com.eightkdata.mongowp.bson.BsonDocument.Entry;
import com.eightkdata.mongowp.bson.abst.AbstractBsonDocument.SimpleEntry;
import com.eightkdata.mongowp.bson.impl.*;
import com.eightkdata.mongowp.bson.utils.DefaultBsonValues;
import com.torodb.kvdocument.values.*;
import com.torodb.kvdocument.values.KvDocument.DocEntry;

import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

/**
 *
 */
public class ToBsonValueTranslator implements KvValueVisitor<BsonValue<?>, Void>,
    Function<KvValue<?>, BsonValue<?>> {

  private ToBsonValueTranslator() {
  }

  @Override
  public BsonValue<?> apply(@Nonnull KvValue<?> kvValue) {
    return kvValue.accept(this, null);
  }

  public static ToBsonValueTranslator getInstance() {
    return ToBsonValueTranslatorHolder.INSTANCE;
  }

  @Override
  public BsonValue<?> visit(KvBoolean value, Void arg) {
    return newBoolean(value.getPrimitiveValue());
  }

  @Override
  public BsonValue<?> visit(KvNull value, Void arg) {
    return DefaultBsonValues.NULL;
  }

  @Override
  public BsonValue<?> visit(KvArray value, Void arg) {
    List<BsonValue<?>> list = new ArrayList<>(value.size());
    for (KvValue<?> kVValue : value) {
      list.add(kVValue.accept(this, arg));
    }
    return newArray(list);
  }

  @Override
  public BsonValue<?> visit(KvInteger value, Void arg) {
    return newInt(value.intValue());
  }

  @Override
  public BsonValue<?> visit(KvLong value, Void arg) {
    return newLong(value.longValue());
  }

  @Override
  public BsonValue<?> visit(KvDouble value, Void arg) {
    return newDouble(value.doubleValue());
  }

  @Override
  public BsonValue<?> visit(KvString value, Void arg) {
    return newString(value.getValue());
  }

  @Override
  public BsonValue<?> visit(KvDocument value, Void arg) {
    List<Entry<?>> entryList = new ArrayList<>(value.size());
    for (DocEntry<?> docEntry : value) {
      entryList.add(new SimpleEntry<>(
          docEntry.getKey(),
          docEntry.getValue().accept(this, arg))
      );
    }
    return newDocument(entryList);
  }

  @Override
  public BsonValue<?> visit(KvMongoObjectId value, Void arg) {
    return new ByteArrayBsonObjectId(value.getArrayValue());
  }

  @Override
  public BsonValue<?> visit(KvInstant value, Void arg) {
    return new LongBsonDateTime(value.getMillisFromUnix());
  }

  @Override
  public BsonValue<?> visit(KvDate value, Void arg) {
    List<Entry<?>> entryList = new ArrayList<>(2);
    entryList.add(new SimpleEntry<>("type", newString("KVDate")));
    entryList.add(new SimpleEntry<>("value", newString(value.getValue().format(
        DateTimeFormatter.ISO_DATE))));

    return newDocument(entryList);
  }

  @Override
  public BsonValue<?> visit(KvTime value, Void arg) {
    List<Entry<?>> entryList = new ArrayList<>(2);
    entryList.add(new SimpleEntry<>("type", newString("KVTime")));
    entryList.add(new SimpleEntry<>("value", newString(value.getValue().format(
        DateTimeFormatter.ISO_TIME))));

    return newDocument(entryList);
  }

  @Override
  public BsonBinary visit(KvBinary value, Void arg) {
    BinarySubtype subtype;
    switch (value.getSubtype()) {
      case MONGO_FUNCTION:
        subtype = BinarySubtype.FUNCTION;
        break;
      case MONGO_GENERIC:
        subtype = BinarySubtype.GENERIC;
        break;
      case MONGO_MD5:
        subtype = BinarySubtype.MD5;
        break;
      case MONGO_OLD_BINARY:
        subtype = BinarySubtype.OLD_BINARY;
        break;
      case MONGO_OLD_UUID:
        subtype = BinarySubtype.OLD_UUID;
        break;
      case MONGO_USER_DEFINED:
        subtype = BinarySubtype.USER_DEFINED;
        break;
      case MONGO_UUID:
        subtype = BinarySubtype.UUID;
        break;
      case UNDEFINED:
        subtype = BinarySubtype.USER_DEFINED;
        break;
      default:
        throw new AssertionError("It is not defined how to translate "
            + "the binary subtype " + value.getSubtype() + " to"
            + "MongoDB binaries subtypes");
    }
    byte byteType = value.getCategory();
    return new ByteArrayBsonBinary(subtype, byteType, value.getByteSource().read());
  }

  @Override
  public BsonTimestamp visit(KvMongoTimestamp value, Void arg) {
    return new DefaultBsonTimestamp(value.getSecondsSinceEpoch(), value.getOrdinal());
  }

  @Override
  public BsonValue<?> visit(KvDecimal128 value, Void arg) {
    return newDecimal128(value.getHigh(), value.getLow());
  }

  @Override
  public BsonJavaScript visit(KvMongoJavascript value, Void arg) {
    return new DefaultBsonJavaScript(value.getValue());
  }

  @Override
  public DefaultBsonJavaScriptWithCode visit(KvMongoJavascriptWithScope value, Void arg) {
    //We don't support this because our string
    throw new UnsupportedBsonTypeException(BsonType.JAVA_SCRIPT_WITH_SCOPE);
  }

  @Override
  public BsonMin visit(KvMinKey value, Void arg) {
    return SimpleBsonMin.getInstance();
  }

  @Override
  public BsonMax visit(KvMaxKey value, Void arg) {
    return SimpleBsonMax.getInstance();
  }

  @Override
  public BsonUndefined visit(KvUndefined value, Void arg) {
    return SimpleBsonUndefined.getInstance();
  }

  @Override
  public BsonRegex visit(KvMongoRegex value, Void arg) {

    List<BsonRegex.Options> kvOptions = value.getOptions().stream().map(
            bsonOption -> BsonRegex.Options.valueOf(bsonOption.name())).collect(Collectors.toList());

    return new DefaultBsonRegex(
            kvOptions.isEmpty()?EnumSet.noneOf(BsonRegex.Options.class):EnumSet.copyOf(kvOptions),
            value.getPattern()
    );


  }

  @Override
  public BsonDbPointer visit(KvMongoDbPointer value, Void arg) {
    return new DefaultBsonDbPointer(
            value.getNamespace(),
            (BsonObjectId) visit(value.getId(),arg)
    );
  }

  @Override
  public BsonValue<?> visit(KvDeprecated value, Void arg) {
    return new StringBsonDeprecated(value.getValue());
  }

  private static class ToBsonValueTranslatorHolder {

    private static final ToBsonValueTranslator INSTANCE = new ToBsonValueTranslator();
  }

  //@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD")
  private Object readResolve() {
    return ToBsonValueTranslator.getInstance();
  }

}
