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
import com.torodb.kvdocument.types.JavascriptWithScopeType;
import com.torodb.kvdocument.types.KvType;
import com.torodb.kvdocument.values.KvMongoJavascriptWithScope;

import java.io.ByteArrayInputStream;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

/** */
public class MongoJavascriptWithScopeValueConverter
    implements KvValueConverter<String, String, KvMongoJavascriptWithScope> {

  private static final long serialVersionUID = 1L;

  public static final DataTypeForKv<KvMongoJavascriptWithScope> TYPE =
      JsonbBinding.fromKvValue(
          KvMongoJavascriptWithScope.class, new MongoJavascriptWithScopeValueConverter());

  @Override
  public KvType getErasuredType() {
    return JavascriptWithScopeType.INSTANCE;
  }

  @Override
  public KvMongoJavascriptWithScope from(String databaseObject) {

    final JsonReader reader =
        Json.createReader(new ByteArrayInputStream(databaseObject.getBytes()));
    JsonObject object = reader.readObject();

    //need to discuss implementation of scope
    return KvMongoJavascriptWithScope.of(object.getString("js"), object.getString("scope"));
  }

  @Override
  public String to(KvMongoJavascriptWithScope userObject) {
    return Json.createObjectBuilder()
        .add("js", userObject.getJs())
        .add("scope", userObject.getScope())
        .build()
        .toString();
  }

  @Override
  public Class<String> fromType() {
    return String.class;
  }

  @Override
  public Class<KvMongoJavascriptWithScope> toType() {
    return KvMongoJavascriptWithScope.class;
  }

  @Override
  public SqlBinding<String> getSqlBinding() {
    return JsonbSqlBinding.INSTANCE;
  }
}
