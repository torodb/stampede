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

package com.torodb.backend.derby.tables.records;

import com.torodb.backend.converters.TableRefConverter;
import com.torodb.backend.derby.tables.DerbyMetaDocPartTable;
import com.torodb.backend.tables.records.MetaDocPartRecord;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;

import javax.json.JsonArray;

public class DerbyMetaDocPartRecord extends MetaDocPartRecord<JsonArray> {

  private static final long serialVersionUID = 4525720333148409410L;

  /**
   * Create a detached MetaDocPartRecord
   */
  public DerbyMetaDocPartRecord() {
    super(DerbyMetaDocPartTable.DOC_PART);
  }

  /**
   * Create a detached, initialised MetaDocPartRecord
   */
  public DerbyMetaDocPartRecord(String database, String collection, JsonArray tableRef,
      String identifier, Integer lastRid) {
    super(DerbyMetaDocPartTable.DOC_PART);
    values(database, collection, tableRef, identifier, lastRid);
  }

  @Override
  public DerbyMetaDocPartRecord values(String database, String collection, JsonArray tableRef,
      String identifier, Integer lastRid) {
    setDatabase(database);
    setCollection(collection);
    setTableRef(tableRef);
    setIdentifier(identifier);
    setLastRid(lastRid);
    return this;
  }

  @Override
  protected JsonArray toTableRefType(TableRef tableRef) {
    return TableRefConverter.toJsonArray(tableRef);
  }

  @Override
  public TableRef getTableRefValue(TableRefFactory tableRefFactory) {
    return TableRefConverter.fromJsonArray(tableRefFactory, getTableRef());
  }
}
