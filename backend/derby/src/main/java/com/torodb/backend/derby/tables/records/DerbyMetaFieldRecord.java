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
package com.torodb.backend.derby.tables.records;

import com.torodb.backend.derby.tables.DerbyMetaFieldTable;
import com.torodb.backend.tables.records.MetaFieldRecord;

public class DerbyMetaFieldRecord extends MetaFieldRecord<String[]> {

    private static final long serialVersionUID = -7296241344455399566L;

    /**
	 * Create a detached MetaFieldRecord
	 */
	public DerbyMetaFieldRecord() {
		super(DerbyMetaFieldTable.FIELD);
	}

	/**
	 * Create a detached, initialised MetaFieldRecord
	 */
	public DerbyMetaFieldRecord(String database, String collection, String[] tableRef, String name, String identifier, String type) {
		super(DerbyMetaFieldTable.FIELD);
		
		values(database, collection, tableRef, name, identifier, type);
	}

    @Override
    public MetaFieldRecord values(String database, String collection, String[] tableRef, String name, String identifier, String type) {
        setDatabase(database);
        setCollection(collection);
        setTableRef(tableRef);
        setName(name);
        setIdentifier(identifier);
        setType(type);
        return this;
    }
}
