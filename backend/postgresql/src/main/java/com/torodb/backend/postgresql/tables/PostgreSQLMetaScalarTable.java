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
import org.jooq.impl.SQLDataType;
import org.jooq.util.postgres.PostgresDataType;

import com.torodb.backend.converters.jooq.FieldTypeConverter;
import com.torodb.backend.postgresql.tables.records.PostgreSQLMetaScalarRecord;
import com.torodb.backend.tables.MetaScalarTable;
import com.torodb.core.transaction.metainf.FieldType;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = {"EQ_DOESNT_OVERRIDE_EQUALS","HE_HASHCODE_NO_EQUALS"})
public class PostgreSQLMetaScalarTable extends MetaScalarTable<String[], PostgreSQLMetaScalarRecord> {

    private static final long serialVersionUID = -2338985946298600866L;
    /**
	 * The singleton instance of <code>torodb.collections</code>
	 */
	public static final PostgreSQLMetaScalarTable SCALAR = new PostgreSQLMetaScalarTable();

	@Override
    public Class<PostgreSQLMetaScalarRecord> getRecordType() {
        return PostgreSQLMetaScalarRecord.class;
    }
	
	/**
	 * Create a <code>torodb.collections</code> table reference
	 */
	public PostgreSQLMetaScalarTable() {
		this(TABLE_NAME, null);
	}

	/**
	 * Create an aliased <code>torodb.collections</code> table reference
	 */
	public PostgreSQLMetaScalarTable(String alias) {
	    this(alias, PostgreSQLMetaScalarTable.SCALAR);
	}

	private PostgreSQLMetaScalarTable(String alias, Table<PostgreSQLMetaScalarRecord> aliased) {
		this(alias, aliased, null);
	}

	private PostgreSQLMetaScalarTable(String alias, Table<PostgreSQLMetaScalarRecord> aliased, Field<?>[] parameters) {
		super(alias, aliased, parameters);
	}
    
	/**
	 * {@inheritDoc}
	 */
	@Override
	public PostgreSQLMetaScalarTable as(String alias) {
		return new PostgreSQLMetaScalarTable(alias, this);
	}

	/**
	 * Rename this table
	 */
	public PostgreSQLMetaScalarTable rename(String name) {
		return new PostgreSQLMetaScalarTable(name, null);
	}

    @Override
    protected TableField<PostgreSQLMetaScalarRecord, String> createDatabaseField() {
        return createField(TableFields.DATABASE.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLMetaScalarRecord, String> createCollectionField() {
        return createField(TableFields.COLLECTION.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLMetaScalarRecord, String[]> createTableRefField() {
        return createField(TableFields.TABLE_REF.fieldName, PostgresDataType.VARCHAR.getArrayDataType().nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLMetaScalarRecord, FieldType> createTypeField() {
        return createField(TableFields.TYPE.fieldName, FieldTypeConverter.TYPE.nullable(false), this, "");
    }

    @Override
    protected TableField<PostgreSQLMetaScalarRecord, String> createIdentifierField() {
        return createField(TableFields.IDENTIFIER.fieldName, SQLDataType.VARCHAR.nullable(false), this, "");
    }
}
