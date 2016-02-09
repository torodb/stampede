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

import com.torodb.torod.core.subdocument.values.*;
import com.torodb.torod.core.subdocument.values.heap.ListScalarArray;
import com.torodb.torod.db.backends.converters.ValueConverter;
import com.torodb.torod.db.backends.converters.array.ValueToArrayConverterProvider;
import java.util.ArrayList;
import java.util.List;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonValue;

/**
 *
 */
public class ArrayValueToJsonConverter implements
        ValueConverter<JsonArray, ScalarArray> {

    private static final ChildToJson TO_JSON = new ChildToJson();
    
    @Override
    public Class<? extends JsonArray> getJsonClass() {
        return JsonArray.class;
    }

    @Override
    public Class<? extends ScalarArray> getValueClass() {
        return ScalarArray.class;
    }

    @Override
    public JsonArray toJson(ScalarArray value) {
        JsonArrayBuilder builder = Json.createArrayBuilder();
        for (ScalarValue<?> child : value) {
            child.accept(TO_JSON, builder);
        }
        return builder.build();
    }

    @Override
    public ScalarArray toValue(JsonArray value) {
        List<ScalarValue<?>> list = new ArrayList<>(value.size());
        ValueToArrayConverterProvider converterProvider = ValueToArrayConverterProvider.getInstance();
        for (JsonValue child : value) {
            list.add(converterProvider.convertFromJson(child));
        }
        return new ListScalarArray(list);
    }

    private static class ChildToJson implements
            ScalarValueVisitor<Void, JsonArrayBuilder> {

        @Override
        public Void visit(ScalarBoolean value, JsonArrayBuilder arg) {
            arg.add(ValueToArrayConverterProvider.getInstance()
                            .getBooleanConverter().toJson(value)
            );
            return null;
        }

        @Override
        public Void visit(ScalarNull value, JsonArrayBuilder arg) {
            arg.addNull();
            return null;
        }

        @Override
        public Void visit(ScalarArray value, JsonArrayBuilder arg) {
            arg.add(ValueToArrayConverterProvider.getInstance()
                            .getArrayConverter().toJson(value)
            );
            return null;
        }

        @Override
        public Void visit(ScalarInteger value, JsonArrayBuilder arg) {
            arg.add(ValueToArrayConverterProvider.getInstance()
                            .getIntegerConverter().toJson(value)
                            .intValue()
            );
            return null;
        }

        @Override
        public Void visit(ScalarLong value, JsonArrayBuilder arg) {
            arg.add(ValueToArrayConverterProvider.getInstance()
                            .getLongConverter().toJson(value)
                            .longValue()
            );
            return null;
        }

        @Override
        public Void visit(ScalarDouble value, JsonArrayBuilder arg) {
            arg.add(ValueToArrayConverterProvider.getInstance()
                            .getDoubleConverter().toJson(value)
                            .doubleValue()
            );
            return null;
        }

        @Override
        public Void visit(ScalarString value, JsonArrayBuilder arg) {
            arg.add(ValueToArrayConverterProvider.getInstance()
                            .getStringConverter().toJson(value)
            );
            return null;
        }

        @Override
        public Void visit(ScalarMongoObjectId value, JsonArrayBuilder arg) {
            arg.add(ValueToArrayConverterProvider.getInstance()
                            .getMongoObjectIdConverter().toJson(value)
            );
            return null;
        }

        @Override
        public Void visit(ScalarInstant value, JsonArrayBuilder arg) {
            arg.add(ValueToArrayConverterProvider.getInstance()
                            .getInstantConverter().toJson(value)
            );
            return null;
        }

        @Override
        public Void visit(ScalarDate value, JsonArrayBuilder arg) {
            arg.add(ValueToArrayConverterProvider.getInstance()
                            .getDateConverter().toJson(value)
            );
            return null;
        }

        @Override
        public Void visit(ScalarTime value, JsonArrayBuilder arg) {
            arg.add(ValueToArrayConverterProvider.getInstance()
                            .getTimeConverter().toJson(value)
            );
            return null;
        }

        @Override
        public Void visit(ScalarMongoTimestamp value, JsonArrayBuilder arg) {
            arg.add(ValueToArrayConverterProvider.getInstance()
                    .getMongoTimestampConverter().toJson(value)
            );
            return null;
        }

        @Override
        public Void visit(ScalarBinary value, JsonArrayBuilder arg) {
            arg.add(ValueToArrayConverterProvider.getInstance()
                    .getBinaryConverter().toJson(value)
            );
            return null;
        }

    }

}
