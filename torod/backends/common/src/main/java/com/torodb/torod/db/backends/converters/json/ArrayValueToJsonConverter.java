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

package com.torodb.torod.db.backends.converters.json;

import com.torodb.torod.db.backends.converters.ValueConverter;
import com.torodb.torod.core.subdocument.values.*;
import com.torodb.torod.db.backends.converters.array.ValueToArrayConverterProvider;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonValue;

/**
 *
 */
public class ArrayValueToJsonConverter implements
        ValueConverter<JsonArray, ArrayValue> {

    private static final ChildToJson TO_JSON = new ChildToJson();
    
    @Override
    public Class<? extends JsonArray> getJsonClass() {
        return JsonArray.class;
    }

    @Override
    public Class<? extends ArrayValue> getValueClass() {
        return ArrayValue.class;
    }

    @Override
    public JsonArray toJson(ArrayValue value) {
        JsonArrayBuilder builder = Json.createArrayBuilder();
        for (Value<?> child : value) {
            child.accept(TO_JSON, builder);
        }
        return builder.build();
    }

    @Override
    public ArrayValue toValue(JsonArray value) {
        ArrayValue.Builder builder = new ArrayValue.Builder();
        ValueToArrayConverterProvider converterProvider
                = ValueToArrayConverterProvider.getInstance();
        for (JsonValue child : value) {
            builder.add(converterProvider.convertFromJson(child));
        }
        return builder.build();
    }

    private static class ChildToJson implements
            ValueVisitor<Void, JsonArrayBuilder> {

        @Override
        public Void visit(BooleanValue value, JsonArrayBuilder arg) {
            arg.add(ValueToArrayConverterProvider.getInstance()
                            .getBooleanConverter().toJson(value)
            );
            return null;
        }

        @Override
        public Void visit(NullValue value, JsonArrayBuilder arg) {
            arg.addNull();
            return null;
        }

        @Override
        public Void visit(ArrayValue value, JsonArrayBuilder arg) {
            arg.add(ValueToArrayConverterProvider.getInstance()
                            .getArrayConverter().toJson(value)
            );
            return null;
        }

        @Override
        public Void visit(IntegerValue value, JsonArrayBuilder arg) {
            arg.add(ValueToArrayConverterProvider.getInstance()
                            .getIntegerConverter().toJson(value)
                            .intValue()
            );
            return null;
        }

        @Override
        public Void visit(LongValue value, JsonArrayBuilder arg) {
            arg.add(ValueToArrayConverterProvider.getInstance()
                            .getLongConverter().toJson(value)
                            .longValue()
            );
            return null;
        }

        @Override
        public Void visit(DoubleValue value, JsonArrayBuilder arg) {
            arg.add(ValueToArrayConverterProvider.getInstance()
                            .getDoubleConverter().toJson(value)
                            .doubleValue()
            );
            return null;
        }

        @Override
        public Void visit(StringValue value, JsonArrayBuilder arg) {
            arg.add(ValueToArrayConverterProvider.getInstance()
                            .getStringConverter().toJson(value)
            );
            return null;
        }

        @Override
        public Void visit(TwelveBytesValue value, JsonArrayBuilder arg) {
            arg.add(ValueToArrayConverterProvider.getInstance()
                            .getTwelveBytesConverter().toJson(value)
            );
            return null;
        }

        @Override
        public Void visit(DateTimeValue value, JsonArrayBuilder arg) {
            arg.add(ValueToArrayConverterProvider.getInstance()
                            .getDateTimeConverter().toJson(value)
            );
            return null;
        }

        @Override
        public Void visit(DateValue value, JsonArrayBuilder arg) {
            arg.add(ValueToArrayConverterProvider.getInstance()
                            .getDateConverter().toJson(value)
            );
            return null;
        }

        @Override
        public Void visit(TimeValue value, JsonArrayBuilder arg) {
            arg.add(ValueToArrayConverterProvider.getInstance()
                            .getTimeConverter().toJson(value)
            );
            return null;
        }

        @Override
        public Void visit(PatternValue value, JsonArrayBuilder arg) {
            arg.add(ValueToArrayConverterProvider.getInstance()
                    .getPosixConverter().toJson(value)
            );
            return null;
        }

    }

}
