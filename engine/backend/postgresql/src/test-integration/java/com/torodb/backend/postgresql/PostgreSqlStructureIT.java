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

package com.torodb.backend.postgresql;

import com.torodb.backend.common.AbstractStructureIT;
import com.torodb.backend.common.DatabaseTestContext;
import com.torodb.core.transaction.metainf.FieldType;

import java.util.HashMap;
import java.util.Map;

public class PostgreSqlStructureIT extends AbstractStructureIT {

  private Map<FieldType, String> typesDictionary = new HashMap();

  public PostgreSqlStructureIT() {
    typesDictionary.put(FieldType.STRING, "varchar");
    typesDictionary.put(FieldType.BINARY, "bytea");
    typesDictionary.put(FieldType.BOOLEAN, "bool");
    typesDictionary.put(FieldType.DATE, "date");
    typesDictionary.put(FieldType.DOUBLE, "float8");
    typesDictionary.put(FieldType.INSTANT, "timestamptz");
    typesDictionary.put(FieldType.INTEGER, "int4");
    typesDictionary.put(FieldType.LONG, "int8");
    typesDictionary.put(FieldType.MONGO_OBJECT_ID, "bytea");
    typesDictionary.put(FieldType.MONGO_TIME_STAMP, "\"torodb\".\"mongo_timestamp\"");
    typesDictionary.put(FieldType.NULL, "bool");
    typesDictionary.put(FieldType.TIME, "time");
    typesDictionary.put(FieldType.CHILD, "bool");
    typesDictionary.put(FieldType.DECIMAL128, "numeric");
  }

  @Override
  protected DatabaseTestContext getDatabaseTestContext() {
    return new PostgreSqlDatabaseTestContextFactory().createInstance();
  }

  @Override
  protected String getSqlTypeOf(FieldType fieldType) {
    if (!typesDictionary.containsKey(fieldType))
      throw new RuntimeException("Unsupported type " + fieldType.name());

    return typesDictionary.get(fieldType);
  }

}
