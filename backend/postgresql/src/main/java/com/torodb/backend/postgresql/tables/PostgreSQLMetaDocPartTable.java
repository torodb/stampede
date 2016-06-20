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
package com.torodb.backend.postgresql.tables;

import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import com.torodb.backend.postgresql.tables.records.PostgreSQLMetaDocPartRecord;
import com.torodb.backend.tables.MetaDocPartTable;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings("EQ_DOESNT_OVERRIDE_EQUALS")
public class PostgreSQLMetaDocPartTable extends MetaDocPartTable<String[], PostgreSQLMetaDocPartRecord> {

    private static final long serialVersionUID = -550698624070753099L;
    /**
	 * The singleton instance of <code>torodb.collections</code>
	 */
	public static final PostgreSQLMetaDocPartTable DOC_PART = new PostgreSQLMetaDocPartTable();

	@Override
    public Class<PostgreSQLMetaDocPartRecord> getRecordType() {
        return PostgreSQLMetaDocPartRecord.class;
    }
	
	/**
	 * Create a <code>torodb.collections</code> table reference
	 */
	public PostgreSQLMetaDocPartTable() {
		this(TABLE_NAME, null);
	}

	/**
	 * Create an aliased <code>torodb.collections</code> table reference
	 */
	public PostgreSQLMetaDocPartTable(String alias) {
	    this(alias, PostgreSQLMetaDocPartTable.DOC_PART);
	}

	private PostgreSQLMetaDocPartTable(String alias, Table<PostgreSQLMetaDocPartRecord> aliased) {
		this(alias, aliased, null);
	}

	private PostgreSQLMetaDocPartTable(String alias, Table<PostgreSQLMetaDocPartRecord> aliased, Field<?>[] parameters) {
		super(alias, aliased, parameters);
	}
    
	/**
	 * {@inheritDoc}
	 */
	@Override
	public PostgreSQLMetaDocPartTable as(String alias) {
		return new PostgreSQLMetaDocPartTable(alias, this);
	}

	/**
	 * Rename this table
	 */
	public PostgreSQLMetaDocPartTable rename(String name) {
		return new PostgreSQLMetaDocPartTable(name, null);
	}

    @Override
    protected TableField<PostgreSQLMetaDocPartRecord, String> createDatabaseField() {
        return createField(TableFields.DATABASE.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLMetaDocPartRecord, String> createCollectionField() {
        return createField(TableFields.COLLECTION.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLMetaDocPartRecord, String[]> createTableRefField() {
        return createField(TableFields.TABLE_REF.fieldName, SQLDataType.VARCHAR.getArrayDataType().nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLMetaDocPartRecord, String> createIdentifierField() {
        return createField(TableFields.IDENTIFIER.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLMetaDocPartRecord, Integer> createLastRidField() {
        return createField(TableFields.LAST_RID.fieldName, SQLDataType.INTEGER.nullable(false), this, "");
    }

    @Override
    protected Field<Integer> createDidField() {
        return DSL.field(DocPartTableFields.DID.fieldName, SQLDataType.INTEGER.nullable(false));
    }

    @Override
    protected Field<Integer> createRidField() {
        return DSL.field(DocPartTableFields.RID.fieldName, SQLDataType.INTEGER.nullable(false));
    }

    @Override
    protected Field<Integer> createPidField() {
        return DSL.field(DocPartTableFields.PID.fieldName, SQLDataType.INTEGER.nullable(false));
    }

    @Override
    protected Field<Integer> createSeqField() {
        return DSL.field(DocPartTableFields.SEQ.fieldName, SQLDataType.INTEGER.nullable(false));
    }
}
