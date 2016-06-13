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

import javax.json.JsonArray;

import com.torodb.backend.converters.TableRefConverter;
import com.torodb.backend.derby.tables.DerbyMetaFieldTable;
import com.torodb.backend.tables.records.MetaFieldRecord;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.transaction.metainf.FieldType;

public class DerbyMetaFieldRecord extends MetaFieldRecord<JsonArray> {

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
	public DerbyMetaFieldRecord(String database, String collection, JsonArray tableRef, String name, String identifier, FieldType type) {
		super(DerbyMetaFieldTable.FIELD);
		
		values(database, collection, tableRef, name, identifier, type);
	}

    @Override
    public MetaFieldRecord values(String database, String collection, JsonArray tableRef, String name, String identifier, FieldType type) {
        setDatabase(database);
        setCollection(collection);
        setTableRef(tableRef);
        setName(name);
        setIdentifier(identifier);
        setType(type);
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
