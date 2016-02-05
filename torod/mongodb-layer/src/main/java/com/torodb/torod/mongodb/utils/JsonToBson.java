
package com.torodb.torod.mongodb.utils;

import com.eightkdata.mongowp.bson.BsonArray;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonValue;
import com.eightkdata.mongowp.bson.utils.DefaultBsonValues;
import com.eightkdata.mongowp.utils.BsonArrayBuilder;
import com.eightkdata.mongowp.utils.BsonDocumentBuilder;
import java.util.Map.Entry;
import javax.json.*;

import static com.eightkdata.mongowp.bson.BsonType.*;
import static com.eightkdata.mongowp.bson.utils.DefaultBsonValues.*;

/**
 *
 */
public class JsonToBson {

    private JsonToBson() {}

    public static BsonValue<?> transform(JsonValue json) {
        switch (json.getValueType()) {
            case ARRAY:
                assert json instanceof JsonArray;
                return transform((JsonArray) json);
            case FALSE:
                return DefaultBsonValues.FALSE;
            case NULL:
                return DefaultBsonValues.NULL;
            case NUMBER:
                assert json instanceof JsonNumber;
                JsonNumber number = (JsonNumber) json;
                if (number.isIntegral()) {
                    try {
                        long l = number.longValueExact();
                        if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
                            return newLong(number.longValue());
                        }
                        return newInt(number.intValue());
                    } catch (ArithmeticException ex) {
                        throw new AssertionError(json + " is an unrecognized integral number");
                    }
                }
                return newDouble(number.doubleValue());
            case OBJECT:
                assert json instanceof JsonObject;
                return transform((JsonObject) json);
            case STRING:
                return newString(((JsonString) json).getString());
            case TRUE:
                return DefaultBsonValues.TRUE;
            default:
                throw new AssertionError("It is not defined how to translate "
                        + "the unexpected JSON type " + json.getValueType());
        }
    }
    
    public static BsonValue transform(JsonStructure structure) {
        if (structure instanceof JsonArray) {
            return transform((JsonArray) structure);
        }
        if (structure instanceof JsonObject) {
            return transform((JsonObject) structure);
        }
        throw new IllegalArgumentException("Structures whose type is "
                + structure.getClass() + " are not transformable to bson");
    }

    public static BsonArray transform(JsonArray array) {
        BsonArrayBuilder result = new BsonArrayBuilder();
        for (JsonValue val : array) {
            result.add(transform(val));
        }
        return result.build();
    }

    public static BsonDocument transform(JsonObject object) {
        BsonDocumentBuilder result = new BsonDocumentBuilder();
        for (Entry<String, JsonValue> entry : object.entrySet()) {
            result.appendUnsafe(entry.getKey(), transform(entry.getValue()));
        }
        return result.build();
    }

    private static void add(JsonBuilderFun builderFun, BsonValue value) {
        switch (value.getType()) {
            case ARRAY:
                builderFun.add(transform(value.asArray()));
                break;
            case BOOLEAN:
                builderFun.add(value.asBoolean().getValue());
                break;
            case DOCUMENT:
                builderFun.add(transform(value.asDocument()));
                break;
            case DOUBLE:
                builderFun.add(value.asNumber().doubleValue());
                break;
            case INT32:
                builderFun.add(value.asNumber().intValue());
                break;
            case INT64:
                builderFun.add(value.asNumber().longValue());
                break;
            case NULL:
                builderFun.addNull();
                break;
            case STRING:
                builderFun.add(value.asString().getValue());
                break;
            case TIMESTAMP:
            case UNDEFINED:
            case DATETIME:
            case DB_POINTER:
            case BINARY:
            case JAVA_SCRIPT:
            case OBJECT_ID:
            case REGEX:
            case JAVA_SCRIPT_WITH_SCOPE:
            case MAX:
            case MIN:
            default:
                throw new IllegalArgumentException("It is not defined how to "
                        + "transform a " + value.getType() + " to JSON");
        }
    }

    public static JsonArray transform(BsonArray value) {
        JsonArrayBuilderFun builderFun = new JsonArrayBuilderFun(Json.createArrayBuilder());
        for (BsonValue bsonValue : value) {
            add(builderFun, bsonValue);
        }
        return builderFun.builder.build();
    }

    public static JsonObject transform(BsonDocument value) {
        JsonObjectBuilderFun builderFun = new JsonObjectBuilderFun(Json.createObjectBuilder());
        for (com.eightkdata.mongowp.bson.BsonDocument.Entry<?> entry : value) {
            builderFun.setKey(entry.getKey());
            add(builderFun, value);
        }
        return builderFun.builder.build();
    }

    private static interface JsonBuilderFun {
        public void addNull();
        public void add(JsonObject value);
        public void add(JsonArray value);
        public void add(int value);
        public void add(long value);
        public void add(double value);
        public void add(String value);
        public void add(boolean value);
    }

    private static class JsonObjectBuilderFun implements JsonBuilderFun {

        private final JsonObjectBuilder builder;
        private String key;

        public JsonObjectBuilderFun(JsonObjectBuilder builder) {
            this.builder = builder;
        }

        public void setKey(String key) {
            this.key = key;
        }

        @Override
        public void addNull() {
            builder.addNull(key);
        }

        @Override
        public void add(JsonObject value) {
            builder.add(key, value);
        }

        @Override
        public void add(JsonArray value) {
            builder.add(key, value);
        }

        @Override
        public void add(int value) {
            builder.add(key, value);
        }

        @Override
        public void add(long value) {
            builder.add(key, value);
        }

        @Override
        public void add(double value) {
            builder.add(key, value);
        }

        @Override
        public void add(String value) {
            builder.add(key, value);
        }

        @Override
        public void add(boolean value) {
            builder.add(key, value);
        }

    }

    private static class JsonArrayBuilderFun implements JsonBuilderFun {
        private final JsonArrayBuilder builder;

        public JsonArrayBuilderFun(JsonArrayBuilder builder) {
            this.builder = builder;
        }

        @Override
        public void addNull() {
            builder.addNull();
        }

        @Override
        public void add(JsonObject value) {
            builder.add(value);
        }

        @Override
        public void add(JsonArray value) {
            builder.add(value);
        }

        @Override
        public void add(int value) {
            builder.add(value);
        }

        @Override
        public void add(long value) {
            builder.add(value);
        }

        @Override
        public void add(double value) {
            builder.add(value);
        }

        @Override
        public void add(String value) {
            builder.add(value);
        }

        @Override
        public void add(boolean value) {
            builder.add(value);
        }
    }
}
