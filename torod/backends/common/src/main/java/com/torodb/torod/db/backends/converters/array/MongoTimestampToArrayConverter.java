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

package com.torodb.torod.db.backends.converters.array;

import javax.json.Json;
import javax.json.JsonObject;

import com.torodb.torod.core.subdocument.values.ScalarMongoTimestamp;
import com.torodb.torod.core.subdocument.values.heap.DefaultScalarMongoTimestamp;
import com.torodb.torod.db.backends.udt.MongoTimestampUDT;

/**
 *
 */
public class MongoTimestampToArrayConverter implements ArrayConverter<JsonObject, ScalarMongoTimestamp> {
    private static final long serialVersionUID = 1L;

    private static final String SECS = MongoTimestampUDT.SECS.getName();
    private static final String COUNTER = MongoTimestampUDT.COUNTER.getName();

    @Override
    public String toJsonLiteral(ScalarMongoTimestamp value) {
        return Json.createObjectBuilder()
                .add(SECS, value.getSecondsSinceEpoch())
                .add(COUNTER, value.getOrdinal())
                .build().toString();
    }

    @Override
    public ScalarMongoTimestamp fromJsonValue(JsonObject value) {
        assert isValid(value);
        return new DefaultScalarMongoTimestamp(value.getInt(SECS), value.getInt(COUNTER));
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
