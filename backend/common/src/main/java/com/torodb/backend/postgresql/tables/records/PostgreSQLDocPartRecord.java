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
package com.torodb.backend.postgresql.tables.records;

import com.torodb.backend.postgresql.tables.PostgreSQLDocPartTable;
import com.torodb.backend.tables.records.MetaDocPartRecord;

public class PostgreSQLDocPartRecord extends MetaDocPartRecord<String[]> {

    private static final long serialVersionUID = 4525720333148409410L;

    /**
	 * Create a detached MetaDocPartRecord
	 */
	public PostgreSQLDocPartRecord() {
		super(PostgreSQLDocPartTable.CONTAINER);
	}

	/**
	 * Create a detached, initialised MetaDocPartRecord
	 */
	public PostgreSQLDocPartRecord(String database, String collection, String[] tableRef, String identifierName, Integer lastRid) {
		super(PostgreSQLDocPartTable.CONTAINER);
		
	}

    @Override
    public PostgreSQLDocPartRecord values(String database, String collection, String[] tableRef, String identifier, Integer lastRid) {
        setDatabase(database);
        setCollection(collection);
        setTableRef(tableRef);
        setIdentifier(identifier);
        setLastRid(lastRid);
        return this;
    }
}
