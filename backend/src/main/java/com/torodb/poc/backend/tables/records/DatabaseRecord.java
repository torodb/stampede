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
package com.torodb.poc.backend.tables.records;

import org.jooq.Field;
import org.jooq.Record1;
import org.jooq.Record2;
import org.jooq.Row2;
import org.jooq.impl.UpdatableRecordImpl;

import com.torodb.poc.backend.tables.DatabaseTable;

public abstract class DatabaseRecord extends UpdatableRecordImpl<DatabaseRecord> 
        implements Record2<String, String> {
    
    private static final long serialVersionUID = -3134779659016002480L;

    /**
     * Setter for <code>torodb.database.name</code>.
     */
    public void setName(String value) {
        setValue(0, value);
    }

    /**
     * Getter for <code>torodb.database.name</code>.
     */
    public String getName() {
        return (String) getValue(0);
    }

    /**
     * Setter for <code>torodb.database.schema_name</code>.
     */
    public void setSchemaName(String value) {
        setValue(1, value);
    }

    /**
     * Getter for <code>torodb.database.schema_name</code>.
     */
    public String getSchemaName() {
        return (String) getValue(1);
    }

	// -------------------------------------------------------------------------
	// Primary key information
	// -------------------------------------------------------------------------

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Record1<String> key() {
		return (Record1) super.key();
	}

	// -------------------------------------------------------------------------
	// Record7 type implementation
	// -------------------------------------------------------------------------

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Row2<String, String> fieldsRow() {
		return (Row2) super.fieldsRow();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Row2<String, String> valuesRow() {
		return (Row2) super.valuesRow();
	}

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<String> field1() {
        return databaseTable.NAME;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<String> field2() {
        return databaseTable.SCHEMA_NAME;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String value1() {
        return getName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String value2() {
        return getSchemaName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DatabaseRecord value1(String value) {
        setName(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DatabaseRecord value2(String value) {
        setSchemaName(value);
        return this;
    }


	/**
	 * {@inheritDoc}
	 */
	@Override
	public DatabaseRecord values(String value1, String value2) {
		return this;
	}

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    private final DatabaseTable databaseTable;
    
    /**
     * Create a detached DatabaseRecord
     */
    public DatabaseRecord(DatabaseTable databaseTable) {
        super(databaseTable);
        
        this.databaseTable = databaseTable;
    }
}
