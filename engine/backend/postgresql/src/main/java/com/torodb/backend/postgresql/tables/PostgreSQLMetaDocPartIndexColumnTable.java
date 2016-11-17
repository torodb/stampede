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

import com.torodb.backend.converters.jooq.OrderingConverter;
import com.torodb.backend.postgresql.tables.records.PostgreSQLMetaDocPartIndexColumnRecord;
import com.torodb.backend.tables.MetaDocPartIndexColumnTable;
import com.torodb.core.transaction.metainf.FieldIndexOrdering;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = {"EQ_DOESNT_OVERRIDE_EQUALS","HE_HASHCODE_NO_EQUALS"})
public class PostgreSQLMetaDocPartIndexColumnTable extends MetaDocPartIndexColumnTable<String[], PostgreSQLMetaDocPartIndexColumnRecord> {

    private static final long serialVersionUID = -426812622031112992L;
    /**
	 * The singleton instance of <code>torodb.field_index</code>
	 */
	public static final PostgreSQLMetaDocPartIndexColumnTable DOC_PART_INDEX_COLUMN = new PostgreSQLMetaDocPartIndexColumnTable();

	@Override
    public Class<PostgreSQLMetaDocPartIndexColumnRecord> getRecordType() {
        return PostgreSQLMetaDocPartIndexColumnRecord.class;
    }
	
	/**
	 * Create a <code>torodb.collections</code> table reference
	 */
	public PostgreSQLMetaDocPartIndexColumnTable() {
		this(TABLE_NAME, null);
	}

	/**
	 * Create an aliased <code>torodb.collections</code> table reference
	 */
	public PostgreSQLMetaDocPartIndexColumnTable(String alias) {
	    this(alias, PostgreSQLMetaDocPartIndexColumnTable.DOC_PART_INDEX_COLUMN);
	}

	private PostgreSQLMetaDocPartIndexColumnTable(String alias, Table<PostgreSQLMetaDocPartIndexColumnRecord> aliased) {
		this(alias, aliased, null);
	}

	private PostgreSQLMetaDocPartIndexColumnTable(String alias, Table<PostgreSQLMetaDocPartIndexColumnRecord> aliased, Field<?>[] parameters) {
		super(alias, aliased, parameters);
	}
    
	/**
	 * {@inheritDoc}
	 */
	@Override
	public PostgreSQLMetaDocPartIndexColumnTable as(String alias) {
		return new PostgreSQLMetaDocPartIndexColumnTable(alias, this);
	}

	/**
	 * Rename this table
	 */
	public PostgreSQLMetaDocPartIndexColumnTable rename(String name) {
		return new PostgreSQLMetaDocPartIndexColumnTable(name, null);
	}

    @Override
    protected TableField<PostgreSQLMetaDocPartIndexColumnRecord, String> createDatabaseField() {
        return createField(TableFields.DATABASE.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLMetaDocPartIndexColumnRecord, String> createCollectionField() {
        return createField(TableFields.COLLECTION.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLMetaDocPartIndexColumnRecord, String> createIndexIdentifierField() {
        return createField(TableFields.INDEX_IDENTIFIER.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLMetaDocPartIndexColumnRecord, String[]> createTableRefField() {
        return createField(TableFields.TABLE_REF.fieldName, SQLDataType.VARCHAR.getArrayDataType().nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLMetaDocPartIndexColumnRecord, Integer> createPositionField() {
        return createField(TableFields.POSITION.fieldName, SQLDataType.INTEGER.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLMetaDocPartIndexColumnRecord, String> createIdentifierField() {
        return createField(TableFields.IDENTIFIER.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLMetaDocPartIndexColumnRecord, FieldIndexOrdering> createOrderingField() {
        return createField(TableFields.ORDERING.fieldName, OrderingConverter.TYPE.nullable(false), this, "");
    }

}
