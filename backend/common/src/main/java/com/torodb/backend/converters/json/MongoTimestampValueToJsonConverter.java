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
 * along with common. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.backend.converters.json;

import javax.json.JsonObject;

import com.torodb.backend.converters.ValueConverter;
import com.torodb.backend.udt.MongoTimestampUDT;
import com.torodb.kvdocument.values.KVMongoTimestamp;
import com.torodb.kvdocument.values.heap.DefaultKVMongoTimestamp;

/**
 *
 */
public class MongoTimestampValueToJsonConverter implements
        ValueConverter<JsonObject, KVMongoTimestamp> {

    private static final long serialVersionUID = 1L;

    private static final String SECS = MongoTimestampUDT.SECS.getName();
    private static final String COUNTER = MongoTimestampUDT.COUNTER.getName();
    
    @Override
    public Class<? extends JsonObject> getJsonClass() {
        return JsonObject.class;
    }

    @Override
    public Class<? extends KVMongoTimestamp> getValueClass() {
        return KVMongoTimestamp.class;
    }

    @Override
    public KVMongoTimestamp toValue(JsonObject value) {
        assert isValid(value);
        return new DefaultKVMongoTimestamp(value.getInt(SECS), value.getInt(COUNTER));
    }

    public boolean isValid(JsonObject object) {
        try {
            object.getInt(SECS);
            object.getInt(COUNTER);
            return true;
        } catch (NullPointerException | ClassCastException ex) {
            return false;
        }
    }

}
