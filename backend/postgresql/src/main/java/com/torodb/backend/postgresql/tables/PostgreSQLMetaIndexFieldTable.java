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

import com.torodb.backend.converters.jooq.OrderingConverter;
import com.torodb.backend.postgresql.tables.records.PostgreSQLMetaIndexFieldRecord;
import com.torodb.backend.tables.MetaIndexFieldTable;
import com.torodb.core.transaction.metainf.FieldIndexOrdering;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = {"EQ_DOESNT_OVERRIDE_EQUALS","HE_HASHCODE_NO_EQUALS"})
public class PostgreSQLMetaIndexFieldTable extends MetaIndexFieldTable<String[], PostgreSQLMetaIndexFieldRecord> {

    private static final long serialVersionUID = 8649935905000022435L;
    /**
	 * The singleton instance of <code>torodb.collections</code>
	 */
	public static final PostgreSQLMetaIndexFieldTable INDEX_FIELD = new PostgreSQLMetaIndexFieldTable();

	@Override
    public Class<PostgreSQLMetaIndexFieldRecord> getRecordType() {
        return PostgreSQLMetaIndexFieldRecord.class;
    }
	
	/**
	 * Create a <code>torodb.collections</code> table reference
	 */
	public PostgreSQLMetaIndexFieldTable() {
		this(TABLE_NAME, null);
	}

	/**
	 * Create an aliased <code>torodb.collections</code> table reference
	 */
	public PostgreSQLMetaIndexFieldTable(String alias) {
	    this(alias, PostgreSQLMetaIndexFieldTable.INDEX_FIELD);
	}

	private PostgreSQLMetaIndexFieldTable(String alias, Table<PostgreSQLMetaIndexFieldRecord> aliased) {
		this(alias, aliased, null);
	}

	private PostgreSQLMetaIndexFieldTable(String alias, Table<PostgreSQLMetaIndexFieldRecord> aliased, Field<?>[] parameters) {
		super(alias, aliased, parameters);
	}
    
	/**
	 * {@inheritDoc}
	 */
	@Override
	public PostgreSQLMetaIndexFieldTable as(String alias) {
		return new PostgreSQLMetaIndexFieldTable(alias, this);
	}

	/**
	 * Rename this table
	 */
	public PostgreSQLMetaIndexFieldTable rename(String name) {
		return new PostgreSQLMetaIndexFieldTable(name, null);
	}

    @Override
    protected TableField<PostgreSQLMetaIndexFieldRecord, String> createDatabaseField() {
        return createField(TableFields.DATABASE.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLMetaIndexFieldRecord, String> createCollectionField() {
        return createField(TableFields.COLLECTION.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLMetaIndexFieldRecord, String> createIndexField() {
        return createField(TableFields.INDEX.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLMetaIndexFieldRecord, Integer> createPositionField() {
        return createField(TableFields.POSITION.fieldName, SQLDataType.INTEGER.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLMetaIndexFieldRecord, String[]> createTableRefField() {
        return createField(TableFields.TABLE_REF.fieldName, SQLDataType.VARCHAR.getArrayDataType().nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLMetaIndexFieldRecord, String> createNameField() {
        return createField(TableFields.NAME.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLMetaIndexFieldRecord, FieldIndexOrdering> createOrderingField() {
        return createField(TableFields.ORDERING.fieldName, OrderingConverter.TYPE.nullable(false), this, "");
    }
}
