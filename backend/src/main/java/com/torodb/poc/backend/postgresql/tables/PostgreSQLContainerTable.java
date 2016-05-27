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

import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.SQLDataType;

import com.torodb.poc.backend.postgresql.tables.records.PostgreSQLContainerRecord;
import com.torodb.poc.backend.tables.ContainerTable;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings("EQ_DOESNT_OVERRIDE_EQUALS")
public class PostgreSQLContainerTable extends ContainerTable<PostgreSQLContainerRecord> {

    private static final long serialVersionUID = -550698624070753099L;
    /**
	 * The singleton instance of <code>torodb.collections</code>
	 */
	public static final PostgreSQLContainerTable CONTAINER = new PostgreSQLContainerTable();

	@Override
    public Class<PostgreSQLContainerRecord> getRecordType() {
        return PostgreSQLContainerRecord.class;
    }
	
	/**
	 * Create a <code>torodb.collections</code> table reference
	 */
	public PostgreSQLContainerTable() {
		this(TABLE_NAME, null);
	}

	/**
	 * Create an aliased <code>torodb.collections</code> table reference
	 */
	public PostgreSQLContainerTable(String alias) {
	    this(alias, PostgreSQLContainerTable.CONTAINER);
	}

	private PostgreSQLContainerTable(String alias, Table<PostgreSQLContainerRecord> aliased) {
		this(alias, aliased, null);
	}

	private PostgreSQLContainerTable(String alias, Table<PostgreSQLContainerRecord> aliased, Field<?>[] parameters) {
		super(alias, aliased, parameters);
	}
    
	/**
	 * {@inheritDoc}
	 */
	@Override
	public PostgreSQLContainerTable as(String alias) {
		return new PostgreSQLContainerTable(alias, this);
	}

	/**
	 * Rename this table
	 */
	public PostgreSQLContainerTable rename(String name) {
		return new PostgreSQLContainerTable(name, null);
	}

    @Override
    protected TableField<PostgreSQLContainerRecord, String> createDatabaseField() {
        return createField(TableFields.DATABASE.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLContainerRecord, String> createCollectionField() {
        return createField(TableFields.COLLECTION.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLContainerRecord, String> createPathField() {
        return createField(TableFields.PATH.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLContainerRecord, String> createTableNameField() {
        return createField(TableFields.TABLE_NAME.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLContainerRecord, Integer> createLastRidField() {
        return createField(TableFields.LAST_RID.fieldName, SQLDataType.INTEGER.nullable(false), this, "");
    }
}
