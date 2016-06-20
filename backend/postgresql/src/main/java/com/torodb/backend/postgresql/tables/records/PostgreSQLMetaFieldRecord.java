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

import com.torodb.backend.converters.TableRefConverter;
import com.torodb.backend.postgresql.tables.PostgreSQLMetaFieldTable;
import com.torodb.backend.tables.records.MetaFieldRecord;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.transaction.metainf.FieldType;

public class PostgreSQLMetaFieldRecord extends MetaFieldRecord<String[]> {

    private static final long serialVersionUID = -7296241344455399566L;

    /**
	 * Create a detached MetaFieldRecord
	 */
	public PostgreSQLMetaFieldRecord() {
		super(PostgreSQLMetaFieldTable.FIELD);
	}

	/**
	 * Create a detached, initialised MetaFieldRecord
	 */
	public PostgreSQLMetaFieldRecord(String database, String collection, String[] tableRef, String name, FieldType type, String identifier) {
		super(PostgreSQLMetaFieldTable.FIELD);
		
		values(database, collection, tableRef, name, type, identifier);
	}

    @Override
    public MetaFieldRecord values(String database, String collection, String[] tableRef, String name, FieldType type, String identifier) {
        setDatabase(database);
        setCollection(collection);
        setTableRef(tableRef);
        setName(name);
        setType(type);
        setIdentifier(identifier);
        return this;
    }

    @Override
    protected String[] toTableRefType(TableRef tableRef) {
        return TableRefConverter.toStringArray(tableRef);
    }

    @Override
    public TableRef getTableRefValue(TableRefFactory tableRefFactory) {
        return TableRefConverter.fromStringArray(tableRefFactory, getTableRef());
    }
}
