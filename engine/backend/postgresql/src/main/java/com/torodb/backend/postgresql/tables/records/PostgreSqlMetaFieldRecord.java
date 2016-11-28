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

package com.torodb.backend.postgresql.tables.records;

import com.torodb.backend.converters.TableRefConverter;
import com.torodb.backend.postgresql.tables.PostgreSqlMetaFieldTable;
import com.torodb.backend.tables.records.MetaFieldRecord;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.transaction.metainf.FieldType;

public class PostgreSqlMetaFieldRecord extends MetaFieldRecord<String[]> {

  private static final long serialVersionUID = -7296241344455399566L;

  /**
   * Create a detached MetaFieldRecord
   */
  public PostgreSqlMetaFieldRecord() {
    super(PostgreSqlMetaFieldTable.FIELD);
  }

  /**
   * Create a detached, initialised MetaFieldRecord
   */
  public PostgreSqlMetaFieldRecord(String database, String collection, String[] tableRef,
      String name, FieldType type, String identifier) {
    super(PostgreSqlMetaFieldTable.FIELD);

    values(database, collection, tableRef, name, type, identifier);
  }

  @Override
  public MetaFieldRecord<String[]> values(String database, String collection, String[] tableRef,
      String name, FieldType type, String identifier) {
    setDatabase(database);
    setCollection(collection);
    setTableRef(tableRef);
    setName(name);
    setType(type);
    setIdentifier(identifier);
    return this;
  }

  @Override
  protected String[] toTableRefType(TableRef tableRef) {
    return TableRefConverter.toStringArray(tableRef);
  }

  @Override
  public TableRef getTableRefValue(TableRefFactory tableRefFactory) {
    return TableRefConverter.fromStringArray(tableRefFactory, getTableRef());
  }
}
