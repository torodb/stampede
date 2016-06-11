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
package com.torodb.torod.db.backends.mysql.tables;

import org.jooq.Converters;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.DefaultDataType;
import org.jooq.impl.SQLDataType;

import com.torodb.torod.db.backends.DatabaseInterface;
import com.torodb.torod.db.backends.converters.jooq.JSONBBinding;
import com.torodb.torod.db.backends.mysql.tables.records.MySQLCollectionsRecord;
import com.torodb.torod.db.backends.tables.AbstractCollectionsTable;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings("EQ_DOESNT_OVERRIDE_EQUALS")
public class MySQLCollectionsTable extends AbstractCollectionsTable<MySQLCollectionsRecord> {

    private static final long serialVersionUID = 304258902776870571L;
    /**
	 * The singleton instance of <code>torodb.collections</code>
	 */
	public static final MySQLCollectionsTable COLLECTIONS = new MySQLCollectionsTable();

	@Override
    public Class<MySQLCollectionsRecord> getRecordType() {
        return MySQLCollectionsRecord.class;
    }
	
	/**
	 * Create a <code>torodb.collections</code> table reference
	 */
	public MySQLCollectionsTable() {
		this(TABLE_NAME, null);
	}

	/**
	 * Create an aliased <code>torodb.collections</code> table reference
	 */
	public MySQLCollectionsTable(String alias) {
	    this(alias, MySQLCollectionsTable.COLLECTIONS);
	}

	private MySQLCollectionsTable(String alias, Table<MySQLCollectionsRecord> aliased) {
		this(alias, aliased, null);
	}

	private MySQLCollectionsTable(String alias, Table<MySQLCollectionsRecord> aliased, Field<?>[] parameters) {
		super(alias, aliased, parameters);
	}
    
    public String getSQLCreationStatement(DatabaseInterface databaseInterface) {
        return databaseInterface.createCollectionsTableStatement(getSchema().getName(), getName());
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public MySQLCollectionsTable as(String alias) {
		return new MySQLCollectionsTable(alias, this);
	}

	/**
	 * Rename this table
	 */
	public MySQLCollectionsTable rename(String name) {
		return new MySQLCollectionsTable(name, null);
	}

    @Override
    protected TableField<MySQLCollectionsRecord, String> createNameField() {
        return createField(TableFields.NAME.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<MySQLCollectionsRecord, String> createSchemaField() {
        return createField(TableFields.SCHEMA.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<MySQLCollectionsRecord, Boolean> createCappedField() {
        return createField(TableFields.CAPPED.fieldName, SQLDataType.BOOLEAN.nullable(false), this, "");
    }

    @Override
    protected TableField<MySQLCollectionsRecord, Integer> createMaxSizeField() {
        return createField(TableFields.MAX_SIZE.fieldName, SQLDataType.INTEGER.nullable(false), this, "");
    }

    @Override
    protected TableField<MySQLCollectionsRecord, Integer> createMaxElementsField() {
        return createField(TableFields.MAX_ELEMENTS.fieldName, SQLDataType.INTEGER.nullable(false), this, "");
    }

    @Override
    protected TableField<MySQLCollectionsRecord, String> createOtherField() {
        return createField(TableFields.OTHER.fieldName, new DefaultDataType<String>(null, String.class, "json"), this, "");
    }

    @Override
    protected TableField<MySQLCollectionsRecord, String> createStorageEngineField() {
        return createField(TableFields.STORAGE_ENGINE.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }
}
