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

import com.torodb.backend.postgresql.tables.records.PostgreSQLFieldRecord;
import com.torodb.backend.tables.MetaFieldTable;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings("EQ_DOESNT_OVERRIDE_EQUALS")
public class DerbyFieldTable extends MetaFieldTable<String[], PostgreSQLFieldRecord> {

    private static final long serialVersionUID = 2305519627765737325L;
    /**
	 * The singleton instance of <code>torodb.collections</code>
	 */
	public static final DerbyFieldTable FIELD = new DerbyFieldTable();

	@Override
    public Class<PostgreSQLFieldRecord> getRecordType() {
        return PostgreSQLFieldRecord.class;
    }
	
	/**
	 * Create a <code>torodb.collections</code> table reference
	 */
	public DerbyFieldTable() {
		this(TABLE_NAME, null);
	}

	/**
	 * Create an aliased <code>torodb.collections</code> table reference
	 */
	public DerbyFieldTable(String alias) {
	    this(alias, DerbyFieldTable.FIELD);
	}

	private DerbyFieldTable(String alias, Table<PostgreSQLFieldRecord> aliased) {
		this(alias, aliased, null);
	}

	private DerbyFieldTable(String alias, Table<PostgreSQLFieldRecord> aliased, Field<?>[] parameters) {
		super(alias, aliased, parameters);
	}
    
	/**
	 * {@inheritDoc}
	 */
	@Override
	public DerbyFieldTable as(String alias) {
		return new DerbyFieldTable(alias, this);
	}

	/**
	 * Rename this table
	 */
	public DerbyFieldTable rename(String name) {
		return new DerbyFieldTable(name, null);
	}

    @Override
    protected TableField<PostgreSQLFieldRecord, String> createDatabaseField() {
        return createField(TableFields.DATABASE.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLFieldRecord, String> createCollectionField() {
        return createField(TableFields.COLLECTION.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLFieldRecord, String[]> createTableRefField() {
        return createField(TableFields.TABLE_REF.fieldName, SQLDataType.VARCHAR.getArrayDataType().nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLFieldRecord, String> createNameField() {
        return createField(TableFields.NAME.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLFieldRecord, String> createIdentifierField() {
        return createField(TableFields.IDENTIFIER.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLFieldRecord, String> createTypeField() {
        return createField(TableFields.TYPE.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }
}
