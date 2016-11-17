/*
 * ToroDB - ToroDB: Backend PostgreSQL
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

import com.torodb.backend.postgresql.tables.records.PostgreSQLMetaCollectionRecord;
import com.torodb.backend.tables.MetaCollectionTable;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = {"EQ_DOESNT_OVERRIDE_EQUALS","HE_HASHCODE_NO_EQUALS"})
public class PostgreSQLMetaCollectionTable extends MetaCollectionTable<PostgreSQLMetaCollectionRecord> {

    private static final long serialVersionUID = 304258902776870571L;
    /**
	 * The singleton instance of <code>torodb.collections</code>
	 */
	public static final PostgreSQLMetaCollectionTable COLLECTION = new PostgreSQLMetaCollectionTable();

	@Override
    public Class<PostgreSQLMetaCollectionRecord> getRecordType() {
        return PostgreSQLMetaCollectionRecord.class;
    }
	
	/**
	 * Create a <code>torodb.collections</code> table reference
	 */
	public PostgreSQLMetaCollectionTable() {
		this(TABLE_NAME, null);
	}

	/**
	 * Create an aliased <code>torodb.collections</code> table reference
	 */
	public PostgreSQLMetaCollectionTable(String alias) {
	    this(alias, PostgreSQLMetaCollectionTable.COLLECTION);
	}

	private PostgreSQLMetaCollectionTable(String alias, Table<PostgreSQLMetaCollectionRecord> aliased) {
		this(alias, aliased, null);
	}

	private PostgreSQLMetaCollectionTable(String alias, Table<PostgreSQLMetaCollectionRecord> aliased, Field<?>[] parameters) {
		super(alias, aliased, parameters);
	}
    
	/**
	 * {@inheritDoc}
	 */
	@Override
	public PostgreSQLMetaCollectionTable as(String alias) {
		return new PostgreSQLMetaCollectionTable(alias, this);
	}

	/**
	 * Rename this table
	 */
	public PostgreSQLMetaCollectionTable rename(String name) {
		return new PostgreSQLMetaCollectionTable(name, null);
	}

    @Override
    protected TableField<PostgreSQLMetaCollectionRecord, String> createDatabaseField() {
        return createField(TableFields.DATABASE.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLMetaCollectionRecord, String> createNameField() {
        return createField(TableFields.NAME.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLMetaCollectionRecord, String> createIdentifierField() {
        return createField(TableFields.IDENTIFIER.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }
}
