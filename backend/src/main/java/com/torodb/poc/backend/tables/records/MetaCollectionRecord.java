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
import org.jooq.Record2;
import org.jooq.Row2;
import org.jooq.impl.UpdatableRecordImpl;

import com.torodb.poc.backend.tables.MetaCollectionTable;

public abstract class MetaCollectionRecord extends UpdatableRecordImpl<MetaCollectionRecord> 
        implements Record2<String, String> {
// database, name
	private static final long serialVersionUID = -2107968478;

    /**
     * Setter for <code>torodb.collection.database</code>.
     */
    public void setDatabase(String value) {
        setValue(0, value);
    }

    /**
     * Getter for <code>torodb.collection.database</code>.
     */
    public String getDatabase() {
        return (String) getValue(0);
    }

    /**
     * Setter for <code>torodb.collection.name</code>.
     */
    public void setName(String value) {
        setValue(1, value);
    }

    /**
     * Getter for <code>torodb.collection.name</code>.
     */
    public String getName() {
        return (String) getValue(1);
    }

	// -------------------------------------------------------------------------
	// Primary key information
	// -------------------------------------------------------------------------

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Record2<String, String> key() {
		return (Record2) super.key();
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
        return metaCollectionTable.DATABASE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<String> field2() {
        return metaCollectionTable.NAME;
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
        return getName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaCollectionRecord value1(String value) {
        setName(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaCollectionRecord value2(String value) {
        setName(value);
        return this;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public abstract MetaCollectionRecord values(String value1, String value2);

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    private final MetaCollectionTable metaCollectionTable;
    
    /**
     * Create a detached MetaCollectionRecord
     */
    public MetaCollectionRecord(MetaCollectionTable metaCollectionTable) {
        super(metaCollectionTable);
        
        this.metaCollectionTable = metaCollectionTable;
    }
}
