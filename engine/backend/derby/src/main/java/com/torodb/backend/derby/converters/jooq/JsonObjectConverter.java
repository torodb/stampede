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
import javax.json.JsonObject;

/**
 *
 */
public class JsonObjectConverter implements Converter<String, JsonObject> {

  private static final long serialVersionUID = 1L;

  public static final DataType<JsonObject> TYPE = StringValueConverter.VARCHAR_TYPE
      .asConvertedDataType(new JsonObjectConverter());

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
