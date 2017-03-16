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

package com.torodb.backend.postgresql.converters.jooq;

import com.torodb.backend.converters.jooq.DataTypeForKv;
import com.torodb.backend.converters.jooq.KvValueConverter;
import com.torodb.backend.converters.sql.SqlBinding;
import com.torodb.backend.postgresql.converters.jooq.binding.JsonbBinding;
import com.torodb.backend.postgresql.converters.sql.JsonbSqlBinding;
import com.torodb.kvdocument.types.KvType;
import com.torodb.kvdocument.types.MongoRegexType;
import com.torodb.kvdocument.values.KvMongoRegex;

import java.io.ByteArrayInputStream;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

/** */
public class MongoRegexValueConverter implements KvValueConverter<String, String, KvMongoRegex> {

  private static final long serialVersionUID = 1L;

  public static final DataTypeForKv<KvMongoRegex> TYPE =
      JsonbBinding.fromKvValue(KvMongoRegex.class, new MongoRegexValueConverter());

  @Override
  public KvType getErasuredType() {
    return MongoRegexType.INSTANCE;
  }

  @Override
  public KvMongoRegex from(String databaseObject) {
    final JsonReader reader =
        Json.createReader(new ByteArrayInputStream(databaseObject.getBytes()));
    JsonObject object = reader.readObject();

    return KvMongoRegex.of(object.getString("pattern"), object.getString("options"));
  }

  @Override
  public String to(KvMongoRegex userObject) {
    return Json.createObjectBuilder()
        .add("pattern", userObject.getPattern())
        .add("options", userObject.getOptionsAsText())
        .build()
        .toString();
  }

  @Override
  public Class<String> fromType() {
    return String.class;
  }

  @Override
  public Class<KvMongoRegex> toType() {
    return KvMongoRegex.class;
  }

  @Override
  public SqlBinding<String> getSqlBinding() {
    return JsonbSqlBinding.INSTANCE;
  }
}
