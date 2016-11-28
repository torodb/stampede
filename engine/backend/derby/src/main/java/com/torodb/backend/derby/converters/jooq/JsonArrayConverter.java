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

package com.torodb.backend.derby.converters.jooq;

import org.jooq.Converter;
import org.jooq.DataType;

import java.io.StringReader;

import javax.json.Json;
import javax.json.JsonArray;

/**
 *
 */
public class JsonArrayConverter implements Converter<String, JsonArray> {

  private static final long serialVersionUID = 1L;

  public static final DataType<JsonArray> TYPE = StringValueConverter.VARCHAR_TYPE
      .asConvertedDataType(new JsonArrayConverter());

  @Override
  public JsonArray from(String databaseObject) {
    return Json.createReader(new StringReader(databaseObject)).readArray();
  }

  @Override
  public String to(JsonArray userObject) {
    return userObject.toString();
  }

  @Override
  public Class<String> fromType() {
    return String.class;
  }

  @Override
  public Class<JsonArray> toType() {
    return JsonArray.class;
  }

}
