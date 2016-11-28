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
import com.torodb.backend.derby.tables.DerbyMetaFieldIndexTable;
import com.torodb.backend.tables.records.MetaFieldIndexRecord;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.transaction.metainf.FieldIndexOrdering;
import com.torodb.core.transaction.metainf.FieldType;

import javax.json.JsonArray;

public class DerbyMetaFieldIndexRecord extends MetaFieldIndexRecord<JsonArray> {

  private static final long serialVersionUID = -4698190965933336115L;

  /**
   * Create a detached MetaFieldRecord
   */
  public DerbyMetaFieldIndexRecord() {
    super(DerbyMetaFieldIndexTable.FIELD_INDEX);
  }

  /**
   * Create a detached, initialised MetaFieldRecord
   */
  public DerbyMetaFieldIndexRecord(String database, String identifier, String collection,
      Integer position, JsonArray tableRef, String name, FieldType type,
      FieldIndexOrdering fieldIndexOrdering) {
    super(DerbyMetaFieldIndexTable.FIELD_INDEX);

    values(database, identifier, position, collection, tableRef, name, type, fieldIndexOrdering);
  }

  @Override
  public MetaFieldIndexRecord<JsonArray> values(String database, String identifier, 
      Integer position, String collection, JsonArray tableRef, String name, FieldType type,
      FieldIndexOrdering fieldIndexOrdering) {
    setDatabase(database);
    setIdentifier(identifier);
    setPosition(position);
    setCollection(collection);
    setTableRef(tableRef);
    setName(name);
    setType(type);
    setOrdering(fieldIndexOrdering);
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
