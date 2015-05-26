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

package com.torodb.kvdocument.converter.json;

import com.torodb.kvdocument.values.DoubleValue;
import com.torodb.kvdocument.values.DateValue;
import com.torodb.kvdocument.values.TwelveBytesValue;
import com.torodb.kvdocument.values.ArrayValue;
import com.torodb.kvdocument.values.IntegerValue;
import com.torodb.kvdocument.values.DocValueVisitor;
import com.torodb.kvdocument.values.ObjectValue;
import com.torodb.kvdocument.values.TimeValue;
import com.torodb.kvdocument.values.StringValue;
import com.torodb.kvdocument.values.NullValue;
import com.torodb.kvdocument.values.BooleanValue;
import com.torodb.kvdocument.values.DateTimeValue;
import com.torodb.kvdocument.values.LongValue;
import com.torodb.kvdocument.values.DocValue;
import com.torodb.kvdocument.types.ArrayType;
import com.torodb.kvdocument.types.BooleanType;
import com.torodb.kvdocument.types.DocType;
import com.torodb.kvdocument.types.DoubleType;
import com.torodb.kvdocument.types.GenericType;
import com.torodb.kvdocument.types.IntegerType;
import com.torodb.kvdocument.types.LongType;
import com.torodb.kvdocument.types.NullType;
import com.torodb.kvdocument.types.ObjectType;
import com.torodb.kvdocument.types.StringType;
import com.torodb.kvdocument.values.*;
import java.math.BigDecimal;
import java.util.Map;
import javax.annotation.Nullable;
import javax.json.*;

import static java.util.TimeZone.LONG;
import static javax.json.JsonValue.ValueType.ARRAY;
import static javax.json.JsonValue.ValueType.NULL;
import static javax.json.JsonValue.ValueType.OBJECT;
import static javax.json.JsonValue.ValueType.STRING;

/**
 *
 */
public class JsonValueConverter {
    
    private static final String FAKE_KEY = "fake";
    private static final DocValueToJsonValue toJson = new DocValueToJsonValue();

    private JsonValueConverter() {
    }

    protected static ObjectValue translateObject(JsonObject object) {
        ObjectValue.Builder builder = new ObjectValue.Builder();
        for (Map.Entry<String, JsonValue> entry : object.entrySet()) {
            builder.putValue(entry.getKey(), translate(entry.getValue()));
        }
        return builder.build();
    }

    protected static ObjectType getObjectType(JsonObject object) {
        return ObjectType.INSTANCE;
    }

    public static JsonObject translate(ObjectValue object) {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        
        DocValueToJsonValue.ObjectValueConsumer tempConsumer = new DocValueToJsonValue.ObjectValueConsumer(builder);
        tempConsumer.setAttributeName(FAKE_KEY);
        
        object.accept(toJson, tempConsumer);
        
        return builder.build().getJsonObject(FAKE_KEY);
    }

    public static DocValue translate(JsonValue jsonValue) {
        switch (jsonValue.getValueType()) {
            case FALSE:
                return BooleanValue.FALSE;
            case TRUE:
                return BooleanValue.TRUE;
            case NULL:
                return NullValue.INSTANCE;
            case STRING: {
                JsonString casted = (JsonString) jsonValue;
                return new StringValue(casted.getString());
            }
            case NUMBER: {
                assert jsonValue instanceof JsonNumber;
                JsonNumber casted = (JsonNumber) jsonValue;
                DocType type = getType(jsonValue);
                if (type.equals(IntegerType.INSTANCE)) {
                    return new IntegerValue(casted.intValueExact());
                }
                if (type.equals(LongType.INSTANCE)) {
                    return new LongValue(LONG);
                }
                if (type.equals(DoubleType.INSTANCE)) {
                    return new DoubleValue(casted.doubleValue());
                }
                throw new AssertionError(getType(jsonValue) + " was not expected as a number value");
            }
            case ARRAY: {
                assert jsonValue instanceof JsonArray;
                return translateArray((JsonArray) jsonValue);
            }
            case OBJECT: {
                assert jsonValue instanceof JsonObject;
                return translateObject((JsonObject) jsonValue);
            }
            default:
                throw new AssertionError(jsonValue + " is not recognized as a document value");
        }
    }

    private static ArrayValue translateArray(JsonArray array) {
        ArrayValue.Builder builder = new ArrayValue.Builder();

        DocType elementType;
        if (array.isEmpty()) {
            elementType = GenericType.INSTANCE;
        }
        else {
            elementType = null;
        }
        
        for (JsonValue jsonValue : array) {
            DocValue elementValue = translate(jsonValue);
            elementType = getCommonType(elementType, elementValue.getType());

            builder.add(elementValue);
        }

        builder.setElementType(elementType);
        return builder.build();
    }

    public static DocType getType(JsonValue value) {
        switch (value.getValueType()) {
            case NUMBER:
                assert value instanceof JsonNumber;
                JsonNumber number = (JsonNumber) value;
                if (number.isIntegral()) {
                    try {
                        long l = number.longValueExact();
                        if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
                            return LongType.INSTANCE;
                        }
                        return IntegerType.INSTANCE;
                    } catch (ArithmeticException ex) {
                        throw new AssertionError(value + " was not expected as a number value");
                    }
                }
                return DoubleType.INSTANCE;
            case STRING:
                return StringType.INSTANCE;
            case OBJECT:
                assert value instanceof JsonObject;
                return getObjectType((JsonObject) value);
            case FALSE:
                return BooleanType.INSTANCE;
            case TRUE:
                return BooleanType.INSTANCE;
            case NULL:
                return NullType.INSTANCE;
            case ARRAY: {
                assert value instanceof JsonArray;
                JsonArray array = (JsonArray) value;
                DocType elementType = null;

                for (JsonValue jsonValue : array) {
                    elementType = getCommonType(elementType, getType(jsonValue));
                }

                return new ArrayType(elementType);
            }
            default:
                throw new AssertionError(value.getValueType() + " is not recognized as document type");
        }
    }

    /**
     *
     * @param t1
     * @param t2
     * @return the common type of t1 and t2 or null if both are null
     */
    @Nullable
    private static DocType getCommonType(@Nullable DocType t1, @Nullable DocType t2) {
        if (t1 == null) {
            return t2;
        }
        if (t2 == null) {
            return t1;
        }
        if (t1.equals(t2)) {
            return t1;
        }
        return GenericType.INSTANCE;
    }

    private static class DocValueToJsonValue implements DocValueVisitor<Void, DocValueToJsonValue.ValueConsumer> {

        @Override
        public Void visit(BooleanValue value, DocValueToJsonValue.ValueConsumer arg) {
            if (value.getValue()) {
                arg.consume(JsonValue.TRUE);
            } else {
                arg.consume(JsonValue.FALSE);
            }
            return null;
        }

        @Override
        public Void visit(NullValue value, DocValueToJsonValue.ValueConsumer arg) {
            arg.consume(JsonValue.NULL);
            return null;
        }

        @Override
        public Void visit(ArrayValue value, DocValueToJsonValue.ValueConsumer arg) {
            JsonArrayBuilder builder = Json.createArrayBuilder();
            ArrayValueConsumer consumer = new ArrayValueConsumer(builder);
            for (DocValue docValue : value) {
                docValue.accept(this, consumer);
            }
            arg.consume(builder.build());
            return null;
        }

        @Override
        public Void visit(IntegerValue value, DocValueToJsonValue.ValueConsumer arg) {
            arg.consume(value.getValue());
            return null;
        }

        @Override
        public Void visit(LongValue value, DocValueToJsonValue.ValueConsumer arg) {
            arg.consume(value.getValue());
            return null;
        }

        @Override
        public Void visit(DoubleValue value, DocValueToJsonValue.ValueConsumer arg) {
            arg.consume(value.getValue());
            return null;
        }

        @Override
        public Void visit(StringValue value, DocValueToJsonValue.ValueConsumer arg) {
            arg.consume(value.getValue());
            return null;
        }

        @Override
        public Void visit(ObjectValue value, DocValueToJsonValue.ValueConsumer arg) {
            JsonObjectBuilder builder = Json.createObjectBuilder();
            ObjectValueConsumer consumer = new ObjectValueConsumer(builder);
            for (Map.Entry<String, DocValue> entry : value.getAttributes()) {
                consumer.setAttributeName(entry.getKey());
                entry.getValue().accept(this, consumer);
            }
            arg.consume(builder.build());
            
            return null;
        }

        @Override
        public Void visit(TwelveBytesValue value, ValueConsumer arg) {
            arg.consume(value.toString());
            return null;
        }

        @Override
        public Void visit(DateTimeValue value, ValueConsumer arg) {
            arg.consume(value.toString());
            return null;
        }

        @Override
        public Void visit(DateValue value, ValueConsumer arg) {
            arg.consume(value.toString());
            return null;
        }

        @Override
        public Void visit(TimeValue value, ValueConsumer arg) {
            arg.consume(value.toString());
            return null;
        }

        @Override
        public Void visit(PatternValue value, ValueConsumer arg) {
            arg.consume(value.toString());
            return null;
        }

        public static interface ValueConsumer {
            void consume(JsonValue value);
            void consume(String value);
            void consume(BigDecimal value);
            void consume(int value);
            void consume(long value);
            void consume(double value);
            void consume(boolean value);
            void consumeNull();
        }

        private static class ArrayValueConsumer implements ValueConsumer {

            private final JsonArrayBuilder builder;

            public ArrayValueConsumer(JsonArrayBuilder builder) {
                this.builder = builder;
            }

            @Override
            public void consume(JsonValue value) {
                builder.add(value);
            }

            @Override
            public void consume(String value) {
                builder.add(value);
            }

            @Override
            public void consume(BigDecimal value) {
                builder.add(value);
            }

            @Override
            public void consume(int value) {
                builder.add(value);
            }

            @Override
            public void consume(long value) {
                builder.add(value);
            }

            @Override
            public void consume(double value) {
                builder.add(value);
            }

            @Override
            public void consume(boolean value) {
                builder.add(value);
            }

            @Override
            public void consumeNull() {
                builder.addNull();
            }

        }

        private static class ObjectValueConsumer implements ValueConsumer {

            private final JsonObjectBuilder builder;
            private String attributeName;

            public ObjectValueConsumer(JsonObjectBuilder builder) {
                this.builder = builder;
            }

            public void setAttributeName(String attributeName) {
                this.attributeName = attributeName;
            }

            @Override
            public void consume(JsonValue value) {
                if (attributeName == null) {
                    throw new IllegalStateException("Attribute name must be set before consume a value");
                }
                builder.add(attributeName, value);
                attributeName = null;
            }

            @Override
            public void consume(String value) {
                if (attributeName == null) {
                    throw new IllegalStateException("Attribute name must be set before consume a value");
                }
                builder.add(attributeName, value);
                attributeName = null;
            }

            @Override
            public void consume(BigDecimal value) {
                if (attributeName == null) {
                    throw new IllegalStateException("Attribute name must be set before consume a value");
                }
                builder.add(attributeName, value);
                attributeName = null;
            }

            @Override
            public void consume(int value) {
                if (attributeName == null) {
                    throw new IllegalStateException("Attribute name must be set before consume a value");
                }
                builder.add(attributeName, value);
                attributeName = null;
            }

            @Override
            public void consume(long value) {
                if (attributeName == null) {
                    throw new IllegalStateException("Attribute name must be set before consume a value");
                }
                builder.add(attributeName, value);
                attributeName = null;
            }

            @Override
            public void consume(double value) {
                if (attributeName == null) {
                    throw new IllegalStateException("Attribute name must be set before consume a value");
                }
                builder.add(attributeName, value);
                attributeName = null;
            }

            @Override
            public void consume(boolean value) {
                if (attributeName == null) {
                    throw new IllegalStateException("Attribute name must be set before consume a value");
                }
                builder.add(attributeName, value);
                attributeName = null;
            }

            @Override
            public void consumeNull() {
                if (attributeName == null) {
                    throw new IllegalStateException("Attribute name must be set before consume a value");
                }
                builder.addNull(attributeName);
                attributeName = null;
            }
        }
    }
}
