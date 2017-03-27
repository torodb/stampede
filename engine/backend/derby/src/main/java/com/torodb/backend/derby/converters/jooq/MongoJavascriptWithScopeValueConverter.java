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

import com.torodb.backend.converters.jooq.DataTypeForKv;
import com.torodb.backend.converters.jooq.KvValueConverter;
import com.torodb.backend.converters.sql.SqlBinding;
import com.torodb.backend.converters.sql.StringSqlBinding;
import com.torodb.kvdocument.types.JavascriptWithScopeType;
import com.torodb.kvdocument.types.KvType;
import com.torodb.kvdocument.values.KvMongoJavascriptWithScope;

import javax.json.Json;
import javax.json.JsonObject;

/**
 *
 */
public class MongoJavascriptWithScopeValueConverter
        implements KvValueConverter<JsonObject, String, KvMongoJavascriptWithScope> {

  private static final long serialVersionUID = 1L;

  public static final DataTypeForKv<KvMongoJavascriptWithScope> TYPE =
          DataTypeForKv.from(
                  JsonObjectConverter.TYPE,
                  new MongoJavascriptWithScopeValueConverter()
          );

  @Override
  public KvType getErasuredType() {
    return JavascriptWithScopeType.INSTANCE;
  }

  @Override
  public KvMongoJavascriptWithScope from(JsonObject databaseObject) {
    return KvMongoJavascriptWithScope.of(
            databaseObject.getString("js"),
            databaseObject.getString("scope")
    );
  }

  @Override
  public JsonObject to(KvMongoJavascriptWithScope userObject) {
    return Json.createObjectBuilder()
            .add("js", userObject.getJs())
            .add("scope", userObject.getScope())
            .build();

  }

  @Override
  public Class<JsonObject> fromType() {
    return JsonObject.class;
  }

  @Override
  public Class<KvMongoJavascriptWithScope> toType() {
    return KvMongoJavascriptWithScope.class;
  }

  @Override
  public SqlBinding<String> getSqlBinding() {
    return StringSqlBinding.INSTANCE;
  }

}
