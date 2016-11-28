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
import com.torodb.backend.postgresql.tables.PostgreSqlMetaScalarTable;
import com.torodb.backend.tables.records.MetaScalarRecord;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.transaction.metainf.FieldType;

public class PostgreSqlMetaScalarRecord extends MetaScalarRecord<String[]> {

  private static final long serialVersionUID = 4277383894798572430L;

  /**
   * Create a detached MetaFieldRecord
   */
  public PostgreSqlMetaScalarRecord() {
    super(PostgreSqlMetaScalarTable.SCALAR);
  }

  /**
   * Create a detached, initialised MetaFieldRecord
   */
  public PostgreSqlMetaScalarRecord(String database, String collection, String[] tableRef,
      FieldType type, String identifier) {
    super(PostgreSqlMetaScalarTable.SCALAR);

    values(database, collection, tableRef, type, identifier);
  }

  @Override
  public MetaScalarRecord<String[]> values(String database, String collection, String[] tableRef,
      FieldType type, String identifier) {
    setDatabase(database);
    setCollection(collection);
    setTableRef(tableRef);
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
