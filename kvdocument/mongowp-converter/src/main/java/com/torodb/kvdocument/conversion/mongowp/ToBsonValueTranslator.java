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

import com.eightkdata.mongowp.bson.BinarySubtype;
import com.eightkdata.mongowp.bson.BsonBinary;
import com.eightkdata.mongowp.bson.BsonDocument.Entry;
import com.eightkdata.mongowp.bson.BsonTimestamp;
import com.eightkdata.mongowp.bson.BsonValue;
import com.eightkdata.mongowp.bson.abst.AbstractBsonDocument.SimpleEntry;
import com.eightkdata.mongowp.bson.impl.ByteArrayBsonBinary;
import com.eightkdata.mongowp.bson.impl.ByteArrayBsonObjectId;
import com.eightkdata.mongowp.bson.impl.DefaultBsonTimestamp;
import com.eightkdata.mongowp.bson.impl.LongBsonDateTime;
import com.torodb.kvdocument.values.KVDocument.DocEntry;
import com.torodb.kvdocument.values.*;
import java.util.ArrayList;
import java.util.List;
import org.threeten.bp.format.DateTimeFormatter;

import static com.eightkdata.mongowp.bson.utils.DefaultBsonValues.*;

/**
 *
 */
public class ToBsonValueTranslator implements KVValueVisitor<BsonValue<?>, Void>{

    private ToBsonValueTranslator() {
    }

    public static ToBsonValueTranslator getInstance() {
        return ToBsonValueTranslatorHolder.INSTANCE;
    }

    @Override
    public BsonValue<?> visit(KVBoolean value, Void arg) {
        return newBoolean(value.getPrimitiveValue());
    }

    @Override
    public BsonValue<?> visit(KVNull value, Void arg) {
        return NULL;
    }

    @Override
    public BsonValue<?> visit(KVArray value, Void arg) {
        List<BsonValue<?>> list = new ArrayList<>(value.size());
        for (KVValue<?> kVValue : value) {
            list.add(kVValue.accept(this, arg));
        }
        return newArray(list);
    }

    @Override
    public BsonValue<?> visit(KVInteger value, Void arg) {
        return newInt(value.intValue());
    }

    @Override
    public BsonValue<?> visit(KVLong value, Void arg) {
        return newLong(value.longValue());
    }

    @Override
    public BsonValue<?> visit(KVDouble value, Void arg) {
        return newDouble(value.doubleValue());
    }

    @Override
    public BsonValue<?> visit(KVString value, Void arg) {
        return newString(value.getValue());
    }

    @Override
    public BsonValue<?> visit(KVDocument value, Void arg) {
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
    public BsonValue<?> visit(KVMongoObjectId value, Void arg) {
        return new ByteArrayBsonObjectId(value.getArrayValue());
    }

    @Override
    public BsonValue<?> visit(KVInstant value, Void arg) {
        return new LongBsonDateTime(value.getMillisFromUnix());
    }

    @Override
    public BsonValue<?> visit(KVDate value, Void arg) {
        List<Entry<?>> entryList = new ArrayList<>(2);
        entryList.add(new SimpleEntry<>("type", newString("KVDate")));
        entryList.add(new SimpleEntry<>("value", newString(value.getValue().format(DateTimeFormatter.ISO_DATE))));

        return newDocument(entryList);
    }

    @Override
    public BsonValue<?> visit(KVTime value, Void arg) {
        List<Entry<?>> entryList = new ArrayList<>(2);
        entryList.add(new SimpleEntry<>("type", newString("KVTime")));
        entryList.add(new SimpleEntry<>("value", newString(value.getValue().format(DateTimeFormatter.ISO_TIME))));

        return newDocument(entryList);
    }

    @Override
    public BsonBinary visit(KVBinary value, Void arg) {
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
    public BsonTimestamp visit(KVMongoTimestamp value, Void arg) {
        return new DefaultBsonTimestamp(value.getSecondsSinceEpoch(), value.getOrdinal());
    }

    private static class ToBsonValueTranslatorHolder {
        private static final ToBsonValueTranslator INSTANCE = new ToBsonValueTranslator();
    }

    //@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD")
    private Object readResolve()  {
        return ToBsonValueTranslator.getInstance();
    }
 }

