/*
 * MongoWP - ToroDB-poc: Backend PostgreSQL
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
package com.torodb.backend.postgresql.tables;

import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.SQLDataType;

import com.torodb.backend.postgresql.tables.records.PostgreSQLMetaDatabaseRecord;
import com.torodb.backend.tables.MetaDatabaseTable;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = {"EQ_DOESNT_OVERRIDE_EQUALS","HE_HASHCODE_NO_EQUALS"})
public class PostgreSQLMetaDatabaseTable extends MetaDatabaseTable<PostgreSQLMetaDatabaseRecord> {

    private static final long serialVersionUID = -5506554761865128847L;
    /**
	 * The singleton instance of <code>torodb.collections</code>
	 */
	public static final PostgreSQLMetaDatabaseTable DATABASE = new PostgreSQLMetaDatabaseTable();

	@Override
    public Class<PostgreSQLMetaDatabaseRecord> getRecordType() {
        return PostgreSQLMetaDatabaseRecord.class;
    }
	
	/**
	 * Create a <code>torodb.collections</code> table reference
	 */
	public PostgreSQLMetaDatabaseTable() {
		this(TABLE_NAME, null);
	}

	/**
	 * Create an aliased <code>torodb.collections</code> table reference
	 */
	public PostgreSQLMetaDatabaseTable(String alias) {
	    this(alias, PostgreSQLMetaDatabaseTable.DATABASE);
	}

	private PostgreSQLMetaDatabaseTable(String alias, Table<PostgreSQLMetaDatabaseRecord> aliased) {
		this(alias, aliased, null);
	}

	private PostgreSQLMetaDatabaseTable(String alias, Table<PostgreSQLMetaDatabaseRecord> aliased, Field<?>[] parameters) {
		super(alias, aliased, parameters);
	}
    
	/**
	 * {@inheritDoc}
	 */
	@Override
	public PostgreSQLMetaDatabaseTable as(String alias) {
		return new PostgreSQLMetaDatabaseTable(alias, this);
	}

	/**
	 * Rename this table
	 */
	public PostgreSQLMetaDatabaseTable rename(String name) {
		return new PostgreSQLMetaDatabaseTable(name, null);
	}

    @Override
    protected TableField<PostgreSQLMetaDatabaseRecord, String> createNameField() {
        return createField(TableFields.NAME.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLMetaDatabaseRecord, String> createIdentifierField() {
        return createField(TableFields.IDENTIFIER.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }
}
