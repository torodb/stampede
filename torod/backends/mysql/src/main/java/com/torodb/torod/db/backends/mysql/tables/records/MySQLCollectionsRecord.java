/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */
package com.torodb.torod.db.backends.mysql.tables.records;

import com.torodb.torod.db.backends.mysql.tables.MySQLCollectionsTable;
import com.torodb.torod.db.backends.tables.records.AbstractCollectionsRecord;

public class MySQLCollectionsRecord extends AbstractCollectionsRecord {

    private static final long serialVersionUID = -6808738482552131596L;

	/**
	 * Create a detached CollectionsRecord
	 */
	public MySQLCollectionsRecord() {
		super(MySQLCollectionsTable.COLLECTIONS);
	}

	/**
	 * Create a detached, initialised CollectionsRecord
	 */
	public MySQLCollectionsRecord(String name, String schema, Boolean capped, Integer maxSize, Integer maxElementes, String other, String storageEngine) {
		super(MySQLCollectionsTable.COLLECTIONS);
		
		setValue(0, name);
		setValue(1, schema);
		setValue(2, capped);
		setValue(3, maxSize);
		setValue(4, maxElementes);
		setValue(5, other);
        setValue(6, storageEngine);
	}
}
