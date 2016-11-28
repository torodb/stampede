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
import com.torodb.backend.postgresql.tables.PostgreSqlMetaDocPartTable;
import com.torodb.backend.tables.records.MetaDocPartRecord;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;

public class PostgreSqlMetaDocPartRecord extends MetaDocPartRecord<String[]> {

  private static final long serialVersionUID = 4525720333148409410L;

  /**
   * Create a detached MetaDocPartRecord
   */
  public PostgreSqlMetaDocPartRecord() {
    super(PostgreSqlMetaDocPartTable.DOC_PART);
  }

  /**
   * Create a detached, initialised MetaDocPartRecord
   */
  public PostgreSqlMetaDocPartRecord(String database, String collection, String[] tableRef,
      String identifier, Integer lastRid) {
    super(PostgreSqlMetaDocPartTable.DOC_PART);

    values(database, collection, tableRef, identifier, lastRid);
  }

  @Override
  public PostgreSqlMetaDocPartRecord values(String database, String collection, String[] tableRef,
      String identifier, Integer lastRid) {
    setDatabase(database);
    setCollection(collection);
    setTableRef(tableRef);
    setIdentifier(identifier);
    setLastRid(lastRid);
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
