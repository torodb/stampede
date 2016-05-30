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
package com.torodb.poc.backend.postgresql.tables.records;

import com.torodb.poc.backend.postgresql.tables.PostgreSQLContainerTable;
import com.torodb.poc.backend.tables.records.ContainerRecord;

public class PostgreSQLContainerRecord extends ContainerRecord {

    private static final long serialVersionUID = 4525720333148409410L;

    /**
	 * Create a detached ContainerRecord
	 */
	public PostgreSQLContainerRecord() {
		super(PostgreSQLContainerTable.CONTAINER);
	}

	/**
	 * Create a detached, initialised ContainerRecord
	 */
	public PostgreSQLContainerRecord(String database, String collection, String path, String tableName, String parentTableName, Integer lastRid) {
		super(PostgreSQLContainerTable.CONTAINER);
		
		setValue(0, database);
		setValue(1, collection);
        setValue(2, path);
        setValue(3, tableName);
        setValue(4, parentTableName);
		setValue(5, lastRid);
	}
}
