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

import com.torodb.backend.postgresql.tables.records.PostgreSQLKvRecord;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.SQLDataType;

import com.torodb.backend.tables.KvTable;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = {"EQ_DOESNT_OVERRIDE_EQUALS","HE_HASHCODE_NO_EQUALS"})
public class PostgreSQLKvTable extends KvTable<PostgreSQLKvRecord> {

    private static final long serialVersionUID = -5506554761865128847L;
    /**
	 * The singleton instance of <code>torodb.collections</code>
	 */
	public static final PostgreSQLKvTable KV = new PostgreSQLKvTable();

	@Override
    public Class<PostgreSQLKvRecord> getRecordType() {
        return PostgreSQLKvRecord.class;
    }

    public PostgreSQLKvTable() {
		this(TABLE_NAME, null);
	}

	public PostgreSQLKvTable(String alias) {
	    this(alias, PostgreSQLKvTable.KV);
	}

	private PostgreSQLKvTable(String alias, Table<PostgreSQLKvRecord> aliased) {
		this(alias, aliased, null);
	}

	private PostgreSQLKvTable(String alias, Table<PostgreSQLKvRecord> aliased, Field<?>[] parameters) {
		super(alias, aliased, parameters);
	}
    
	/**
	 * {@inheritDoc}
	 */
	@Override
	public PostgreSQLKvTable as(String alias) {
		return new PostgreSQLKvTable(alias, this);
	}

	/**
	 * Rename this table
	 */
	public PostgreSQLKvTable rename(String name) {
		return new PostgreSQLKvTable(name, null);
	}

    @Override
    protected TableField<PostgreSQLKvRecord, String> createNameField() {
        return createField(TableFields.KEY.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLKvRecord, String> createIdentifierField() {
        return createField(TableFields.VALUE.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }
}
