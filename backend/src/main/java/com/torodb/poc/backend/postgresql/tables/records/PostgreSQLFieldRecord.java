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

import com.torodb.poc.backend.postgresql.tables.PostgreSQLFieldTable;
import com.torodb.poc.backend.tables.records.FieldRecord;

public class PostgreSQLFieldRecord extends FieldRecord {

    private static final long serialVersionUID = -7296241344455399566L;

    /**
	 * Create a detached FieldRecord
	 */
	public PostgreSQLFieldRecord() {
		super(PostgreSQLFieldTable.FIELD);
	}

	/**
	 * Create a detached, initialised FieldRecord
	 */
	public PostgreSQLFieldRecord(String database, String collection, String path, String name, String columnName, String columnType) {
		super(PostgreSQLFieldTable.FIELD);
		
		values(database, collection, path, name, columnName, columnType);
	}

    @Override
    public FieldRecord values(String database, String collection, String path, String name, String columnName, String columnType) {
        setValue(0, database);
        setValue(1, collection);
        setValue(2, path);
        setValue(3, name);
        setValue(4, columnName);
        setValue(5, columnType);
        return this;
    }
}
