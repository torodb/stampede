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
package com.torodb.backend.derby.tables;

import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.SQLDataType;

import com.torodb.backend.postgresql.tables.records.PostgreSQLDocPartRecord;
import com.torodb.backend.tables.MetaDocPartTable;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings("EQ_DOESNT_OVERRIDE_EQUALS")
public class DerbyDocPartTable extends MetaDocPartTable<String[], PostgreSQLDocPartRecord> {

    private static final long serialVersionUID = -550698624070753099L;
    /**
	 * The singleton instance of <code>torodb.collections</code>
	 */
	public static final DerbyDocPartTable CONTAINER = new DerbyDocPartTable();

	@Override
    public Class<PostgreSQLDocPartRecord> getRecordType() {
        return PostgreSQLDocPartRecord.class;
    }
	
	/**
	 * Create a <code>torodb.collections</code> table reference
	 */
	public DerbyDocPartTable() {
		this(TABLE_NAME, null);
	}

	/**
	 * Create an aliased <code>torodb.collections</code> table reference
	 */
	public DerbyDocPartTable(String alias) {
	    this(alias, DerbyDocPartTable.CONTAINER);
	}

	private DerbyDocPartTable(String alias, Table<PostgreSQLDocPartRecord> aliased) {
		this(alias, aliased, null);
	}

	private DerbyDocPartTable(String alias, Table<PostgreSQLDocPartRecord> aliased, Field<?>[] parameters) {
		super(alias, aliased, parameters);
	}
    
	/**
	 * {@inheritDoc}
	 */
	@Override
	public DerbyDocPartTable as(String alias) {
		return new DerbyDocPartTable(alias, this);
	}

	/**
	 * Rename this table
	 */
	public DerbyDocPartTable rename(String name) {
		return new DerbyDocPartTable(name, null);
	}

    @Override
    protected TableField<PostgreSQLDocPartRecord, String> createDatabaseField() {
        return createField(TableFields.DATABASE.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLDocPartRecord, String> createCollectionField() {
        return createField(TableFields.COLLECTION.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLDocPartRecord, String[]> createTableRefField() {
        return createField(TableFields.TABLE_REF.fieldName, SQLDataType.VARCHAR.getArrayDataType().nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLDocPartRecord, String> createIdentifierField() {
        return createField(TableFields.IDENTIFIER.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLDocPartRecord, Integer> createLastRidField() {
        return createField(TableFields.LAST_RID.fieldName, SQLDataType.INTEGER.nullable(false), this, "");
    }
}
