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

package com.torodb.torod.db.backends.converters.jooq;

import java.io.Serializable;
import java.io.StringReader;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonReader;
import javax.json.JsonValue;

import com.torodb.torod.core.subdocument.ScalarType;
import com.torodb.torod.core.subdocument.values.ScalarArray;
import com.torodb.torod.db.backends.converters.array.ValueToArrayConverterProvider;

/**
 *
 */
public abstract class BaseArrayValueConverter implements
        SubdocValueConverter<String, ScalarArray>, Serializable {

    private static final long serialVersionUID = 1L;

    private final ValueToArrayConverterProvider valueToArrayConverterProvider;
    
    public BaseArrayValueConverter(ValueToArrayConverterProvider valueToArrayConverterProvider) {
        super();
        this.valueToArrayConverterProvider = valueToArrayConverterProvider;
    }

    @Override
    public ScalarType getErasuredType() {
        return ScalarType.ARRAY;
    }

    @SuppressWarnings("unchecked")
    @Override
    public ScalarArray from(String databaseObject) {
        JsonReader reader = Json.createReader(new StringReader(databaseObject));
        
        JsonArray array = reader.readArray();
        
        return (ScalarArray) valueToArrayConverterProvider
                .getConverter(ScalarType.ARRAY).fromJsonValue((JsonValue) array);
    }

    @SuppressWarnings("unchecked")
    @Override
    public String to(ScalarArray userObject) {
        return valueToArrayConverterProvider
                .getConverter(ScalarType.ARRAY).toJsonLiteral(userObject);
    }

    @Override
    public Class<String> fromType() {
        return String.class;
    }

    @Override
    public Class<ScalarArray> toType() {
        return ScalarArray.class;
    }
}
