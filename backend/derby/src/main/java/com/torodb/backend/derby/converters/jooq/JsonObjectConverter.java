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


package com.torodb.backend.derby.converters.jooq;

import java.io.StringReader;

import javax.json.Json;
import javax.json.JsonObject;

import org.jooq.Converter;
import org.jooq.DataType;
import org.jooq.impl.SQLDataType;

/**
 *
 */
public class JsonObjectConverter implements Converter<String, JsonObject>{
    private static final long serialVersionUID = 1L;

    public static final DataType<JsonObject> TYPE = StringValueConverter.VARCHAR_32672.asConvertedDataType(new JsonObjectConverter());

    @Override
    public JsonObject from(String databaseObject) {
        return Json.createReader(new StringReader(databaseObject)).readObject();
    }

    @Override
    public String to(JsonObject userObject) {
        return userObject.toString();
    }

    @Override
    public Class<String> fromType() {
        return String.class;
    }

    @Override
    public Class<JsonObject> toType() {
        return JsonObject.class;
    }
    
}
