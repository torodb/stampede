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

import com.torodb.kvdocument.values.DateTimeValue;
import com.torodb.kvdocument.values.TimeValue;
import com.torodb.kvdocument.values.DocValue;
import com.torodb.kvdocument.values.TwelveBytesValue;
import com.torodb.kvdocument.values.LongValue;
import com.torodb.kvdocument.values.DateValue;
import com.torodb.kvdocument.values.IntegerValue;
import com.torodb.kvdocument.values.DocValueVisitor;
import com.torodb.kvdocument.values.StringValue;
import com.torodb.kvdocument.values.BooleanValue;
import com.torodb.kvdocument.values.DoubleValue;
import com.torodb.kvdocument.values.ObjectValue;
import com.torodb.kvdocument.values.NullValue;
import com.torodb.kvdocument.values.ArrayValue;
import com.google.common.collect.Lists;
import com.torodb.kvdocument.types.GenericType;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;
import org.threeten.bp.*;

/**
 *
 */
public class MongoValueConverter {

    private static final KVValueTranslator KV_VALUE_TRANSATOR
            = new KVValueTranslator();

    public static ObjectValue translateObject(BSONObject object) {
        ObjectValue.Builder builder = new ObjectValue.Builder();
        for (String key : object.keySet()) {
            builder.putValue(key, translateBSON(object.get(key)));
        }
        return builder.build();
    }

    public static ArrayValue translateArray(List array) {
        ArrayValue.Builder builder = new ArrayValue.Builder();
        for (Object object : array) {
            builder.add(translateBSON(object));
        }
        if (array.isEmpty()) {
            builder.setElementType(GenericType.INSTANCE);
        }
        return builder.build();
    }

    public static BSONObject translateObject(ObjectValue object) {
        return (BSONObject) object.accept(KV_VALUE_TRANSATOR, null);
    }

    public static DocValue translateBSON(Object value) {
        if (value instanceof Double) {
            return new DoubleValue((Double) value);
        }
        if (value instanceof String) {
            return new StringValue((String) value);
        }
        if (value instanceof List) {
            List list = (List) value;
            return translateArray(list);
        }
        if (value instanceof BSONObject) {
            return translateObject((BSONObject) value);
        }
        if (value instanceof ObjectId) {
            ObjectId id = (ObjectId) value;
            byte[] bsonBytes = id.toByteArray();
            return new TwelveBytesValue(bsonBytes);
        }
        if (value instanceof Boolean) {
            Boolean bool = (Boolean) value;
            if (bool) {
                return BooleanValue.TRUE;
            }
            return BooleanValue.FALSE;
        }
        if (value instanceof Date) {
            Date date = (Date) value;
            LocalDateTime dateTime = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(date.getTime()),
                    ZoneId.systemDefault()
            );
            return new DateTimeValue(dateTime);
        }
        if (value == null) {
            return NullValue.INSTANCE;
        }
        if (value instanceof Integer) {
            return new IntegerValue((Integer) value);
        }
        if (value instanceof Long) {
            return new LongValue((Long) value);
        }

        throw new IllegalArgumentException("Arguments of " + value.getClass()
                + " type are not supported yet");

    }
    
    private static class KVValueTranslator implements
            DocValueVisitor<Object, Void> {

        @Override
        public Object visit(BooleanValue value,
                            Void arg) {
            return value.getValue();
        }

        @Override
        public Object visit(NullValue value,
                            Void arg) {
            return null;
        }

        @Override
        public Object visit(ArrayValue value,
                            Void arg) {
            List<Object> result = Lists.newArrayListWithCapacity(value.size());
            for (DocValue docValue : value) {
                result.add(docValue.accept(this, arg));
            }
            return result;
        }

        @Override
        public Object visit(IntegerValue value,
                            Void arg) {
            return value.getValue();
        }

        @Override
        public Object visit(LongValue value,
                            Void arg) {
            return value.getValue();
        }

        @Override
        public Object visit(DoubleValue value,
                            Void arg) {
            return value.getValue();
        }

        @Override
        public Object visit(StringValue value,
                            Void arg) {
            return value.getValue();
        }

        @Override
        public Object visit(TwelveBytesValue value,
                            Void arg) {
            byte[] kvBytes = value.getArrayValue();
            return new ObjectId(kvBytes);
        }

        @Override
        public Object visit(DateTimeValue value,
                            Void arg) {
            return DateTimeUtils.toSqlTimestamp(value.getValue());
        }

        @Override
        public Object visit(DateValue value,
                            Void arg) {
            return DateTimeUtils.toSqlDate(value.getValue());
        }

        @Override
        public Object visit(TimeValue value,
                            Void arg) {
            return DateTimeUtils.toSqlTime(value.getValue());
        }

        @Override
        public Object visit(ObjectValue object,
                            Void arg) {
            BSONObject result = new BasicBSONObject(object.getAttributes()
                    .size());

            for (Map.Entry<String, DocValue> entry : object.getAttributes()) {
                result.put(entry.getKey(), entry.getValue().accept(this, arg));
            }

            return result;
        }

    }
}
