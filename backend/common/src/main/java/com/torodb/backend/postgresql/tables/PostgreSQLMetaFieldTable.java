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
package com.torodb.backend.postgresql.tables;

import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.SQLDataType;

import com.torodb.backend.postgresql.tables.records.PostgreSQLMetaFieldRecord;
import com.torodb.backend.tables.MetaFieldTable;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings("EQ_DOESNT_OVERRIDE_EQUALS")
public class PostgreSQLMetaFieldTable extends MetaFieldTable<String[], PostgreSQLMetaFieldRecord> {

    private static final long serialVersionUID = 2305519627765737325L;
    /**
	 * The singleton instance of <code>torodb.collections</code>
	 */
	public static final PostgreSQLMetaFieldTable FIELD = new PostgreSQLMetaFieldTable();

	@Override
    public Class<PostgreSQLMetaFieldRecord> getRecordType() {
        return PostgreSQLMetaFieldRecord.class;
    }
	
	/**
	 * Create a <code>torodb.collections</code> table reference
	 */
	public PostgreSQLMetaFieldTable() {
		this(TABLE_NAME, null);
	}

	/**
	 * Create an aliased <code>torodb.collections</code> table reference
	 */
	public PostgreSQLMetaFieldTable(String alias) {
	    this(alias, PostgreSQLMetaFieldTable.FIELD);
	}

	private PostgreSQLMetaFieldTable(String alias, Table<PostgreSQLMetaFieldRecord> aliased) {
		this(alias, aliased, null);
	}

	private PostgreSQLMetaFieldTable(String alias, Table<PostgreSQLMetaFieldRecord> aliased, Field<?>[] parameters) {
		super(alias, aliased, parameters);
	}
    
	/**
	 * {@inheritDoc}
	 */
	@Override
	public PostgreSQLMetaFieldTable as(String alias) {
		return new PostgreSQLMetaFieldTable(alias, this);
	}

	/**
	 * Rename this table
	 */
	public PostgreSQLMetaFieldTable rename(String name) {
		return new PostgreSQLMetaFieldTable(name, null);
	}

    @Override
    protected TableField<PostgreSQLMetaFieldRecord, String> createDatabaseField() {
        return createField(TableFields.DATABASE.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLMetaFieldRecord, String> createCollectionField() {
        return createField(TableFields.COLLECTION.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLMetaFieldRecord, String[]> createTableRefField() {
        return createField(TableFields.TABLE_REF.fieldName, SQLDataType.VARCHAR.getArrayDataType().nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLMetaFieldRecord, String> createNameField() {
        return createField(TableFields.NAME.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLMetaFieldRecord, String> createIdentifierField() {
        return createField(TableFields.IDENTIFIER.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLMetaFieldRecord, String> createTypeField() {
        return createField(TableFields.TYPE.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }
}
