/*
 * ToroDB - ToroDB-poc: Backend PostgreSQL
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

import com.torodb.backend.postgresql.tables.records.PostgreSQLMetaIndexRecord;
import com.torodb.backend.tables.MetaIndexTable;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = {"EQ_DOESNT_OVERRIDE_EQUALS","HE_HASHCODE_NO_EQUALS"})
public class PostgreSQLMetaIndexTable extends MetaIndexTable<PostgreSQLMetaIndexRecord> {

    private static final long serialVersionUID = -6090026713335495681L;
    /**
	 * The singleton instance of <code>torodb.collections</code>
	 */
	public static final PostgreSQLMetaIndexTable INDEX = new PostgreSQLMetaIndexTable();

	@Override
    public Class<PostgreSQLMetaIndexRecord> getRecordType() {
        return PostgreSQLMetaIndexRecord.class;
    }
	
	/**
	 * Create a <code>torodb.collections</code> table reference
	 */
	public PostgreSQLMetaIndexTable() {
		this(TABLE_NAME, null);
	}

	/**
	 * Create an aliased <code>torodb.collections</code> table reference
	 */
	public PostgreSQLMetaIndexTable(String alias) {
	    this(alias, PostgreSQLMetaIndexTable.INDEX);
	}

	private PostgreSQLMetaIndexTable(String alias, Table<PostgreSQLMetaIndexRecord> aliased) {
		this(alias, aliased, null);
	}

	private PostgreSQLMetaIndexTable(String alias, Table<PostgreSQLMetaIndexRecord> aliased, Field<?>[] parameters) {
		super(alias, aliased, parameters);
	}
    
	/**
	 * {@inheritDoc}
	 */
	@Override
	public PostgreSQLMetaIndexTable as(String alias) {
		return new PostgreSQLMetaIndexTable(alias, this);
	}

	/**
	 * Rename this table
	 */
	public PostgreSQLMetaIndexTable rename(String name) {
		return new PostgreSQLMetaIndexTable(name, null);
	}

    @Override
    protected TableField<PostgreSQLMetaIndexRecord, String> createDatabaseField() {
        return createField(TableFields.DATABASE.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLMetaIndexRecord, String> createCollectionField() {
        return createField(TableFields.COLLECTION.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLMetaIndexRecord, String> createNameField() {
        return createField(TableFields.NAME.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLMetaIndexRecord, Boolean> createUniqueField() {
        return createField(TableFields.UNIQUE.fieldName, SQLDataType.BOOLEAN.nullable(false), this, "");
    }

}
