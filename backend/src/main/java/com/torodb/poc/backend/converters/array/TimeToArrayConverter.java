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

package com.torodb.poc.backend.converters.array;

import java.time.LocalTime;

import javax.json.JsonString;

import org.jooq.tools.json.JSONValue;

import com.torodb.kvdocument.values.KVTime;
import com.torodb.kvdocument.values.heap.LocalTimeKVTime;

/**
 *
 */
public class TimeToArrayConverter implements ArrayConverter<JsonString, KVTime> {
    private static final long serialVersionUID = 1L;

    @Override
    public String toJsonLiteral(KVTime value) {
        return JSONValue.toJSONString(value.toString());
    }

    @Override
    public KVTime fromJsonValue(JsonString value) {
        return new LocalTimeKVTime(LocalTime.parse(value.toString()));
    }
}
