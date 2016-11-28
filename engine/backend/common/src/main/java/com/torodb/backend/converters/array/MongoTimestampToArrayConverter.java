/*
 * ToroDB
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.backend.converters.array;

import com.torodb.backend.udt.MongoTimestampUDT;
import com.torodb.kvdocument.values.KvMongoTimestamp;
import com.torodb.kvdocument.values.heap.DefaultKvMongoTimestamp;

import javax.json.Json;
import javax.json.JsonObject;

/**
 *
 */
public class MongoTimestampToArrayConverter
    implements ArrayConverter<JsonObject, KvMongoTimestamp> {

  private static final long serialVersionUID = 1L;

  private static final String SECS = MongoTimestampUDT.SECS.getName();
  private static final String COUNTER = MongoTimestampUDT.COUNTER.getName();

  @Override
  public String toJsonLiteral(KvMongoTimestamp value) {
    return Json.createObjectBuilder()
        .add(SECS, value.getSecondsSinceEpoch())
        .add(COUNTER, value.getOrdinal())
        .build().toString();
  }

  @Override
  public KvMongoTimestamp fromJsonValue(JsonObject value) {
    assert isValid(value);
    return new DefaultKvMongoTimestamp(value.getInt(SECS), value.getInt(COUNTER));
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
