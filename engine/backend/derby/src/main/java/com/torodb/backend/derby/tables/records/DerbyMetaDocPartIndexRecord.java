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
import com.torodb.backend.derby.tables.DerbyMetaDocPartIndexTable;
import com.torodb.backend.tables.records.MetaDocPartIndexRecord;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;

import javax.json.JsonArray;

public class DerbyMetaDocPartIndexRecord extends MetaDocPartIndexRecord<JsonArray> {

  private static final long serialVersionUID = 9051776163794859336L;

  /**
   * Create a detached MetaIndexRecord
   */
  public DerbyMetaDocPartIndexRecord() {
    super(DerbyMetaDocPartIndexTable.DOC_PART_INDEX);
  }

  @Override
  public DerbyMetaDocPartIndexRecord values(String database, String identifier, String collection,
      JsonArray tableRef, Boolean unique) {
    setDatabase(database);
    setIdentifier(identifier);
    setCollection(collection);
    setTableRef(tableRef);
    setUnique(unique);
    return this;
  }

  /**
   * Create a detached, initialised MetaIndexRecord
   */
  public DerbyMetaDocPartIndexRecord(String database, String identifier, String collection,
      JsonArray tableRef, Boolean unique) {
    super(DerbyMetaDocPartIndexTable.DOC_PART_INDEX);

    values(database, identifier, collection, tableRef, unique);
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
