/*
 * ToroDB - ToroDB: Backend PostgreSQL
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.torodb.backend.postgresql.tables.records;

import com.torodb.backend.postgresql.tables.PostgreSQLMetaCollectionTable;
import com.torodb.backend.tables.records.MetaCollectionRecord;

public class PostgreSQLMetaCollectionRecord extends MetaCollectionRecord {

    private static final long serialVersionUID = -6808738482552131596L;

	/**
	 * Create a detached MetaCollectionRecord
	 */
	public PostgreSQLMetaCollectionRecord() {
		super(PostgreSQLMetaCollectionTable.COLLECTION);
	}

    @Override
    public MetaCollectionRecord values(String database, String name, String identifier) {
        
        setDatabase(database);
        setName(name);
        setIdentifier(identifier);
        return this;
    }

	/**
	 * Create a detached, initialised MetaCollectionRecord
	 */
	public PostgreSQLMetaCollectionRecord(String database, String name, String identifier) {
		super(PostgreSQLMetaCollectionTable.COLLECTION);
		
		values(database, name, identifier);
	}
}
