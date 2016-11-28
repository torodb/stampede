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

import com.torodb.backend.derby.tables.DerbyMetaIndexTable;
import com.torodb.backend.tables.records.MetaIndexRecord;

public class DerbyMetaIndexRecord extends MetaIndexRecord {

  private static final long serialVersionUID = -2922006334416416115L;

  /**
   * Create a detached MetaIndexRecord
   */
  public DerbyMetaIndexRecord() {
    super(DerbyMetaIndexTable.INDEX);
  }

  @Override
  public MetaIndexRecord values(String database, String collection, String name, Boolean unique) {

    setDatabase(database);
    setCollection(collection);
    setName(name);
    setUnique(unique);
    return this;
  }

  /**
   * Create a detached, initialised MetaIndexRecord
   */
  public DerbyMetaIndexRecord(String database, String collection, String name, Boolean unique) {
    super(DerbyMetaIndexTable.INDEX);

    values(database, collection, name, unique);
  }

}
