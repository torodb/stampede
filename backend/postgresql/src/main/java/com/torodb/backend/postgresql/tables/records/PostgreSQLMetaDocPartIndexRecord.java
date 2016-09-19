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
import com.torodb.backend.postgresql.tables.PostgreSQLMetaDocPartIndexTable;
import com.torodb.backend.tables.records.MetaDocPartIndexRecord;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;

public class PostgreSQLMetaDocPartIndexRecord extends MetaDocPartIndexRecord<String[]> {

    private static final long serialVersionUID = 2263619273193694206L;

    /**
	 * Create a detached MetaIndexRecord
	 */
	public PostgreSQLMetaDocPartIndexRecord() {
		super(PostgreSQLMetaDocPartIndexTable.DOC_PART_INDEX);
	}

    @Override
    public PostgreSQLMetaDocPartIndexRecord values(String database, String identifier, String collection, String[] tableRef, Boolean unique) {
        setDatabase(database);
        setIdentifier(identifier);
        setCollection(collection);
        setTableRef(tableRef);
        setUnique(unique);
        return this;
    }

	/**
	 * Create a detached, initialised MetaIndexRecord
	 */
	public PostgreSQLMetaDocPartIndexRecord(String database, String identifier, String collection, String[] tableRef, Boolean unique) {
		super(PostgreSQLMetaDocPartIndexTable.DOC_PART_INDEX);
		
		values(database, identifier, collection, tableRef, unique);
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
