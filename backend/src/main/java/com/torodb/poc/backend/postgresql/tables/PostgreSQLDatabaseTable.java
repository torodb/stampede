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
package com.torodb.poc.backend.postgresql.tables;

import org.jooq.Converters;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.SQLDataType;

import com.torodb.poc.backend.converters.jooq.binging.JSONBBinding;
import com.torodb.poc.backend.postgresql.tables.records.PostgreSQLDatabaseRecord;
import com.torodb.poc.backend.tables.CollectionTable.TableFields;
import com.torodb.poc.backend.tables.DatabaseTable;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings("EQ_DOESNT_OVERRIDE_EQUALS")
public class PostgreSQLDatabaseTable extends DatabaseTable<PostgreSQLDatabaseRecord> {

    private static final long serialVersionUID = -5506554761865128847L;
    /**
	 * The singleton instance of <code>torodb.collections</code>
	 */
	public static final PostgreSQLDatabaseTable DATABASE = new PostgreSQLDatabaseTable();

	@Override
    public Class<PostgreSQLDatabaseRecord> getRecordType() {
        return PostgreSQLDatabaseRecord.class;
    }
	
	/**
	 * Create a <code>torodb.collections</code> table reference
	 */
	public PostgreSQLDatabaseTable() {
		this(TABLE_NAME, null);
	}

	/**
	 * Create an aliased <code>torodb.collections</code> table reference
	 */
	public PostgreSQLDatabaseTable(String alias) {
	    this(alias, PostgreSQLDatabaseTable.DATABASE);
	}

	private PostgreSQLDatabaseTable(String alias, Table<PostgreSQLDatabaseRecord> aliased) {
		this(alias, aliased, null);
	}

	private PostgreSQLDatabaseTable(String alias, Table<PostgreSQLDatabaseRecord> aliased, Field<?>[] parameters) {
		super(alias, aliased, parameters);
	}
    
	/**
	 * {@inheritDoc}
	 */
	@Override
	public PostgreSQLDatabaseTable as(String alias) {
		return new PostgreSQLDatabaseTable(alias, this);
	}

	/**
	 * Rename this table
	 */
	public PostgreSQLDatabaseTable rename(String name) {
		return new PostgreSQLDatabaseTable(name, null);
	}

    @Override
    protected TableField<PostgreSQLDatabaseRecord, String> createNameField() {
        return createField(TableFields.NAME.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLDatabaseRecord, String> createSchemaNameField() {
        return createField(TableFields.SCHEMA_NAME.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }
}
