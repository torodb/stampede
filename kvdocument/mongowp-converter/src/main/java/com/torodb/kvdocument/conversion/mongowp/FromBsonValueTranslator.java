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

import com.eightkdata.mongowp.bson.*;
import com.eightkdata.mongowp.bson.impl.InstantBsonDateTime;
import com.torodb.kvdocument.conversion.mongowp.values.BsonKVString;
import com.torodb.kvdocument.values.KVBinary.KVBinarySubtype;
import com.torodb.kvdocument.values.*;
import com.torodb.kvdocument.values.heap.*;

/**
 *
 */
public class FromBsonValueTranslator implements BsonValueVisitor<KVValue, Void> {

    private FromBsonValueTranslator() {
    }

    public static FromBsonValueTranslator getInstance() {
        return FromBsonValueTranslatorHolder.INSTANCE;
    }


    @Override
    public KVValue visit(BsonArray value, Void arg) {
//        return new LazyBsonKVArray(value);
        return MongoWPConverter.toEagerArray(value);
    }

    @Override
    public KVValue visit(BsonBinary value, Void arg) {
        KVBinarySubtype subtype;
        switch (value.getSubtype()) {
            case FUNCTION:
                subtype = KVBinarySubtype.MONGO_FUNCTION;
                break;
            case GENERIC:
                subtype = KVBinarySubtype.MONGO_GENERIC;
                break;
            case MD5:
                subtype = KVBinarySubtype.MONGO_MD5;
                break;
            case OLD_BINARY:
                subtype = KVBinarySubtype.MONGO_OLD_BINARY;
                break;
            case OLD_UUID:
                subtype = KVBinarySubtype.MONGO_OLD_UUID;
                break;
            case USER_DEFINED:
                subtype = KVBinarySubtype.MONGO_USER_DEFINED;
                break;
            case UUID:
                subtype = KVBinarySubtype.MONGO_UUID;
                break;
            default:
                subtype = KVBinarySubtype.UNDEFINED;
                break;
        }
        return new ByteSourceKVBinary(subtype, value.getNumericSubType(), value.getByteSource().getDelegate());
    }

    @Override
    public KVValue visit(BsonDbPointer value, Void arg) {
        throw new UnsupportedBsonTypeException(value.getType());
    }

    @Override
    public KVValue visit(BsonDateTime value, Void arg) {
        if (value instanceof InstantBsonDateTime) {
            return new InstantKVInstant(value.getValue());
        }
        return new LongKVInstant(value.getMillisFromUnix());
    }

    @Override
    public KVValue visit(BsonDocument value, Void arg) {
//        return new LazyBsonKVDocument(value);
        return MongoWPConverter.toEagerDocument(value);
    }

    @Override
    public KVValue visit(BsonDouble value, Void arg) {
        return KVDouble.of(value.doubleValue());
    }

    @Override
    public KVValue visit(BsonInt32 value, Void arg) {
        return KVInteger.of(value.intValue());
    }

    @Override
    public KVValue visit(BsonInt64 value, Void arg) {
        return KVLong.of(value.longValue());
    }

    @Override
    public KVValue visit(BsonBoolean value, Void arg) {
        if (value.getPrimitiveValue()) {
            return KVBoolean.TRUE;
        }
        return KVBoolean.FALSE;
    }

    @Override
    public KVValue visit(BsonJavaScript value, Void arg) {
        throw new UnsupportedBsonTypeException(value.getType());
    }

    @Override
    public KVValue visit(BsonJavaScriptWithScope value, Void arg) {
        throw new UnsupportedBsonTypeException(value.getType());
    }

    @Override
    public KVValue visit(BsonMax value, Void arg) {
        throw new UnsupportedBsonTypeException(value.getType());
    }

    @Override
    public KVValue visit(BsonMin value, Void arg) {
        throw new UnsupportedBsonTypeException(value.getType());
    }

    @Override
    public KVValue visit(BsonNull value, Void arg) {
        return KVNull.getInstance();
    }

    @Override
    public KVValue visit(BsonObjectId value, Void arg) {
        return new ByteArrayKVMongoObjectId(value.toByteArray());
    }

    @Override
    public KVValue visit(BsonRegex value, Void arg) {
        throw new UnsupportedBsonTypeException(value.getType());
    }

    @Override
    public KVValue visit(BsonString value, Void arg) {
        return new BsonKVString(value);
    }

    @Override
    public KVValue visit(BsonUndefined value, Void arg) {
//        throw new UnsupportedBsonTypeException(value.getType());
        return KVNull.getInstance();
    }

    @Override
    public KVValue visit(BsonTimestamp value, Void arg) {
        return new DefaultKVMongoTimestamp(value.getSecondsSinceEpoch(), value.getOrdinal());
    }

    @Override
    public KVValue visit(BsonDeprecated value, Void arg) {
        throw new UnsupportedBsonTypeException(value.getType());
    }



    private static class FromBsonValueTranslatorHolder {
        private static final FromBsonValueTranslator INSTANCE = new FromBsonValueTranslator();
    }

    //@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD")
    private Object readResolve()  {
        return FromBsonValueTranslator.getInstance();
    }
 }
