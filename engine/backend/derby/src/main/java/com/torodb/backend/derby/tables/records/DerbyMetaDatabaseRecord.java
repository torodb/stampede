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

import com.torodb.backend.derby.tables.DerbyMetaDatabaseTable;
import com.torodb.backend.tables.records.MetaDatabaseRecord;

public class DerbyMetaDatabaseRecord extends MetaDatabaseRecord {

  private static final long serialVersionUID = -7220623531622958067L;

  /**
   * Create a detached MetaCollectionRecord
   */
  public DerbyMetaDatabaseRecord() {
    super(DerbyMetaDatabaseTable.DATABASE);
  }

  /**
   * Create a detached, initialised MetaCollectionRecord
   */
  public DerbyMetaDatabaseRecord(String name, String identifier) {
    super(DerbyMetaDatabaseTable.DATABASE);

    values(name, identifier);
  }

  @Override
  public MetaDatabaseRecord values(String name, String identifier) {
    setName(name);
    setIdentifier(identifier);
    return this;
  }
}
