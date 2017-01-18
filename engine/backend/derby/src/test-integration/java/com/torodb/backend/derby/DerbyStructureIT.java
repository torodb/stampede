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

package com.torodb.backend.derby;

import com.torodb.backend.tests.common.AbstractStructureIntegrationSuite;
import com.torodb.backend.tests.common.DatabaseTestContext;
import com.torodb.core.transaction.metainf.FieldType;

import java.util.HashMap;
import java.util.Map;

public class DerbyStructureIT extends AbstractStructureIntegrationSuite {

  private Map<FieldType, String> typesDictionary = new HashMap();

  public DerbyStructureIT() {
    typesDictionary.put(FieldType.STRING, "VARCHAR");
    typesDictionary.put(FieldType.BINARY, "VARCHAR () FOR BIT DATA");
    typesDictionary.put(FieldType.BOOLEAN, "BOOLEAN");
    typesDictionary.put(FieldType.DATE, "DATE");
    typesDictionary.put(FieldType.DOUBLE, "DOUBLE");
    typesDictionary.put(FieldType.INSTANT, "TIMESTAMP");
    typesDictionary.put(FieldType.INTEGER, "INTEGER");
    typesDictionary.put(FieldType.LONG, "BIGINT");
    typesDictionary.put(FieldType.MONGO_OBJECT_ID, "VARCHAR () FOR BIT DATA");
    typesDictionary.put(FieldType.MONGO_TIME_STAMP, "VARCHAR");
    typesDictionary.put(FieldType.NULL, "BOOLEAN");
    typesDictionary.put(FieldType.TIME, "TIME");
    typesDictionary.put(FieldType.CHILD, "BOOLEAN");
    typesDictionary.put(FieldType.DECIMAL128, "NUMERIC");
  }

  @Override
  protected DatabaseTestContext getDatabaseTestContext() {
    return new DerbyDatabaseTestContextFactory().createInstance();
  }

  @Override
  protected String getSqlTypeOf(FieldType fieldType) {
    if (!typesDictionary.containsKey(fieldType))
      throw new RuntimeException("Unsupported type " + fieldType.name());

    return typesDictionary.get(fieldType);
  }


}
