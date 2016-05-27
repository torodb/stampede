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
import com.torodb.poc.backend.postgresql.tables.records.PostgreSQLCollectionRecord;
import com.torodb.poc.backend.tables.CollectionTable;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings("EQ_DOESNT_OVERRIDE_EQUALS")
public class PostgreSQLCollectionTable extends CollectionTable<PostgreSQLCollectionRecord> {

    private static final long serialVersionUID = 304258902776870571L;
    /**
	 * The singleton instance of <code>torodb.collections</code>
	 */
	public static final PostgreSQLCollectionTable COLLECTION = new PostgreSQLCollectionTable();

	@Override
    public Class<PostgreSQLCollectionRecord> getRecordType() {
        return PostgreSQLCollectionRecord.class;
    }
	
	/**
	 * Create a <code>torodb.collections</code> table reference
	 */
	public PostgreSQLCollectionTable() {
		this(TABLE_NAME, null);
	}

	/**
	 * Create an aliased <code>torodb.collections</code> table reference
	 */
	public PostgreSQLCollectionTable(String alias) {
	    this(alias, PostgreSQLCollectionTable.COLLECTION);
	}

	private PostgreSQLCollectionTable(String alias, Table<PostgreSQLCollectionRecord> aliased) {
		this(alias, aliased, null);
	}

	private PostgreSQLCollectionTable(String alias, Table<PostgreSQLCollectionRecord> aliased, Field<?>[] parameters) {
		super(alias, aliased, parameters);
	}
    
	/**
	 * {@inheritDoc}
	 */
	@Override
	public PostgreSQLCollectionTable as(String alias) {
		return new PostgreSQLCollectionTable(alias, this);
	}

	/**
	 * Rename this table
	 */
	public PostgreSQLCollectionTable rename(String name) {
		return new PostgreSQLCollectionTable(name, null);
	}

    @Override
    protected TableField<PostgreSQLCollectionRecord, String> createDatabaseField() {
        return createField(TableFields.DATABASE.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLCollectionRecord, String> createNameField() {
        return createField(TableFields.NAME.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLCollectionRecord, String> createTableNameField() {
        return createField(TableFields.TABLE_NAME.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLCollectionRecord, Boolean> createCappedField() {
        return createField(TableFields.CAPPED.fieldName, SQLDataType.BOOLEAN.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLCollectionRecord, Integer> createMaxSizeField() {
        return createField(TableFields.MAX_SIZE.fieldName, SQLDataType.INTEGER.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLCollectionRecord, Integer> createMaxElementsField() {
        return createField(TableFields.MAX_ELEMENTS.fieldName, SQLDataType.INTEGER.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLCollectionRecord, String> createOtherField() {
        return createField(TableFields.OTHER.fieldName, JSONBBinding.fromType(String.class, Converters.identity(String.class)), this, "");
    }

    @Override
    protected TableField<PostgreSQLCollectionRecord, String> createStorageEngineField() {
        return createField(TableFields.STORAGE_ENGINE.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLCollectionRecord, Integer> createLastDidField() {
        return createField(TableFields.LAST_DID.fieldName, SQLDataType.INTEGER.nullable(false), this, "");
    }
}
