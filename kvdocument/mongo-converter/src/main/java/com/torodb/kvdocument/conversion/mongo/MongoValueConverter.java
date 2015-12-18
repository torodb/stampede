/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.torodb.kvdocument.conversion.mongo;

import com.google.common.primitives.UnsignedInteger;
import com.torodb.kvdocument.types.GenericType;
import com.torodb.kvdocument.values.StringValue;
import com.torodb.kvdocument.values.*;
import java.util.Map;
import java.util.regex.Pattern;
import org.bson.*;
import org.bson.types.ObjectId;
import org.threeten.bp.Instant;
import org.threeten.bp.LocalDateTime;
import org.threeten.bp.ZoneOffset;

/**
 *
 */
public class MongoValueConverter {

    private static final KVValueTranslator KV_VALUE_TRANSATOR
            = new KVValueTranslator();

    public static ObjectValue translateObject(BsonDocument document) {
        ObjectValue.Builder builder = new ObjectValue.Builder();
        for (String key : document.keySet()) {
            builder.putValue(key, translateBSON(document.get(key)));
        }
        return builder.build();
    }

    public static ArrayValue translateArray(BsonArray array) {
        ArrayValue.Builder builder = new ArrayValue.Builder();
        for (BsonValue object : array) {
            builder.add(translateBSON(object));
        }
        if (array.isEmpty()) {
            builder.setElementType(GenericType.INSTANCE);
        }
        return builder.build();
    }

    public static BsonValue translateDocValue(DocValue docValue) {
        return docValue.accept(KV_VALUE_TRANSATOR, null);
    }

    public static BsonDocument translateObject(ObjectValue object) {
        return (BsonDocument) object.accept(KV_VALUE_TRANSATOR, null);
    }

    public static DocValue translateBSON(BsonValue value) {
        if (value == null || value.isNull()) {
            return NullValue.INSTANCE;
        }
        if (value.isDouble()) {
            return new DoubleValue(value.asDouble().getValue());
        }
        if (value.isString()) {
            return new StringValue(value.asString().getValue());
        }
        if (value.isArray()) {
            return translateArray(value.asArray());
        }
        if (value.isDocument()) {
            return translateObject(value.asDocument());
        }
        if (value.isObjectId()) {
            ObjectId id = value.asObjectId().getValue();
            byte[] bsonBytes = id.toByteArray();
            return new TwelveBytesValue(bsonBytes);
        }
        if (value.isBoolean()) {
            boolean bool = value.asBoolean().getValue();
            if (bool) {
                return BooleanValue.TRUE;
            }
            return BooleanValue.FALSE;
        }
        if (value.isDateTime()) {
            Instant instant = Instant.ofEpochMilli(value.asDateTime().getValue());
            LocalDateTime dateTime = LocalDateTime.ofInstant(
                    instant,
                    ZoneOffset.UTC
            );
            return new DateTimeValue(dateTime);
        }
        if (value.isInt32()) {
            return new IntegerValue(value.asInt32().intValue());
        }
        if (value.isInt64()) {
            return new LongValue(value.asInt64().longValue());
        }
        if (value.isRegularExpression()) {
            BsonRegularExpression exp = value.asRegularExpression();
            return new PatternValue(
                    Pattern.compile(
                            exp.getPattern(),
                            patternOptionsToFlags(exp.getOptions())
                    )
            );
        }

        throw new IllegalArgumentException("Arguments of " + value.getClass()
                + " type are not supported yet");

    }

    public static int patternOptionsToFlags(String options) {
        return 0; //TODO: parse regexp options!
    }

    public static String patternFlagsToOptions(int flags) {
        return ""; //TODO: parse regexp options!
    }

    private static class KVValueTranslator implements
            DocValueVisitor<BsonValue, Void> {

        @Override
        public BsonValue visit(BooleanValue value, Void arg) {
            return BsonBoolean.valueOf(value.getValue());
        }

        @Override
        public BsonValue visit(NullValue value, Void arg) {
            return BsonNull.VALUE;
        }

        @Override
        public BsonValue visit(ArrayValue value, Void arg) {
            BsonArray result = new BsonArray();
            for (DocValue docValue : value) {
                result.add(docValue.accept(this, arg));
            }
            return result;
        }

        @Override
        public BsonValue visit(IntegerValue value, Void arg) {
            return new BsonInt32(value.getValue());
        }

        @Override
        public BsonValue visit(LongValue value, Void arg) {
            return new BsonInt64(value.getValue());
        }

        @Override
        public BsonValue visit(DoubleValue value, Void arg) {
            return new BsonDouble(value.getValue());
        }

        @Override
        public BsonValue visit(StringValue value, Void arg) {
            return new BsonString(value.getValue());
        }

        @Override
        public BsonValue visit(TwelveBytesValue value, Void arg) {
            byte[] kvBytes = value.getArrayValue();
            return new BsonObjectId(new ObjectId(kvBytes));
        }

        @Override
        public BsonValue visit(DateTimeValue value, Void arg) {
            Instant instant = value.getValue().toInstant(ZoneOffset.UTC);
            return new BsonTimestamp(
                    UnsignedInteger.valueOf(instant.getEpochSecond()).intValue(),
                    instant.getNano()
            );
        }

        @Override
        public BsonValue visit(DateValue value, Void arg) {
            Instant instant = value.getValue().atStartOfDay().toInstant(ZoneOffset.UTC);
            return new BsonDateTime(instant.toEpochMilli());
        }

        @Override
        public BsonValue visit(TimeValue value, Void arg) {
            throw new UnsupportedOperationException("Is is not defined how to translate time values to BSON");
        }

        @Override
        public BsonValue visit(PatternValue value, Void arg) {
            return new BsonRegularExpression(
                    value.getValue().pattern(),
                    patternFlagsToOptions(value.getValue().flags())
            );
        }

        @Override
        public BsonValue visit(ObjectValue object, Void arg) {
            BsonDocument result = new BsonDocument();

            for (Map.Entry<String, DocValue> entry : object.getAttributes()) {
                result.put(entry.getKey(), entry.getValue().accept(this, arg));
            }

            return result;
        }

    }
}
