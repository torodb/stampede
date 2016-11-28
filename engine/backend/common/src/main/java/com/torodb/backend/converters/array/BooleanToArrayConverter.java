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

import com.torodb.kvdocument.values.KvBoolean;

import javax.json.JsonValue;

/**
 *
 */
public class BooleanToArrayConverter implements ArrayConverter<JsonValue, KvBoolean> {

  private static final long serialVersionUID = 1L;

  @Override
  public String toJsonLiteral(KvBoolean value) {
    return value.getValue() ? "true" : "false";
  }

  @Override
  public KvBoolean fromJsonValue(JsonValue value) {
    if (value != JsonValue.TRUE && value != JsonValue.FALSE) {
      throw new AssertionError(value + " is not boolean value");
    }

    return KvBoolean.from(value == JsonValue.TRUE);
  }
}
