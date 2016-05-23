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
import org.jooq.Record4;
import org.jooq.Record6;
import org.jooq.Row6;
import org.jooq.impl.UpdatableRecordImpl;

import com.torodb.poc.backend.tables.FieldTable;

public abstract class FieldRecord extends UpdatableRecordImpl<FieldRecord> 
        implements Record6<String, String, String, String, String, String> {
// database, name, original_name, last_did
	private static final long serialVersionUID = -2107968478;

    /**
     * Setter for <code>torodb.field.database</code>.
     */
    public void setDatabase(String value) {
        setValue(0, value);
    }

    /**
     * Getter for <code>torodb.field.database</code>.
     */
    public String getDatabase() {
        return (String) getValue(0);
    }

    /**
     * Setter for <code>torodb.field.collection</code>.
     */
    public void setCollection(String value) {
        setValue(1, value);
    }

    /**
     * Getter for <code>torodb.field.collection</code>.
     */
    public String getCollection() {
        return (String) getValue(1);
    }

    /**
     * Setter for <code>torodb.field.path</code>.
     */
    public void setPath(String value) {
        setValue(2, value);
    }

    /**
     * Getter for <code>torodb.field.path</code>.
     */
    public String getPath() {
        return (String) getValue(2);
    }

    /**
     * Setter for <code>torodb.field.name</code>.
     */
    public void setName(String value) {
        setValue(3, value);
    }

    /**
     * Getter for <code>torodb.field.name</code>.
     */
    public String getName() {
        return (String) getValue(3);
    }

    /**
     * Setter for <code>torodb.field.column_name</code>.
     */
    public void setColumnName(String value) {
        setValue(4, value);
    }

    /**
     * Getter for <code>torodb.field.column_name</code>.
     */
    public String getColumnName() {
        return (String) getValue(4);
    }

    /**
     * Setter for <code>torodb.field.column_type</code>.
     */
    public void setColumnType(String value) {
        setValue(5, value);
    }

    /**
     * Getter for <code>torodb.field.column_type</code>.
     */
    public String getColumnType() {
        return (String) getValue(5);
    }

	// -------------------------------------------------------------------------
	// Primary key information
	// -------------------------------------------------------------------------

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Record4<String, String, String, String> key() {
		return (Record4) super.key();
	}

	// -------------------------------------------------------------------------
	// Record7 type implementation
	// -------------------------------------------------------------------------

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Row6<String, String, String, String, String, String> fieldsRow() {
		return (Row6) super.fieldsRow();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Row6<String, String, String, String, String, String> valuesRow() {
		return (Row6) super.valuesRow();
	}

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<String> field1() {
        return fieldTable.DATABASE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<String> field2() {
        return fieldTable.COLLECTION;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<String> field3() {
        return fieldTable.PATH;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<String> field4() {
        return fieldTable.NAME;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<String> field5() {
        return fieldTable.COLUMN_NAME;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<String> field6() {
        return fieldTable.COLUMN_TYPE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String value1() {
        return getDatabase();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String value2() {
        return getCollection();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String value3() {
        return getPath();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String value4() {
        return getName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String value5() {
        return getColumnName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String value6() {
        return getColumnType();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FieldRecord value1(String value) {
        setDatabase(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FieldRecord value2(String value) {
        setCollection(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FieldRecord value3(String value) {
        setPath(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FieldRecord value4(String value) {
        setName(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FieldRecord value5(String value) {
        setColumnName(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FieldRecord value6(String value) {
        setColumnType(value);
        return this;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FieldRecord values(String value1, String value2, String value3, String value4, String value5, String value6) {
		return this;
	}

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    private final FieldTable fieldTable;
    
    /**
     * Create a detached FieldRecord
     */
    public FieldRecord(FieldTable fieldTable) {
        super(fieldTable);
        
        this.fieldTable = fieldTable;
    }
}
