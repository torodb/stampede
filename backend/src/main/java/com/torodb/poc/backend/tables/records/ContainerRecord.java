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
import org.jooq.Record3;
import org.jooq.Record6;
import org.jooq.Row6;
import org.jooq.impl.UpdatableRecordImpl;

import com.torodb.poc.backend.tables.ContainerTable;

public abstract class ContainerRecord extends UpdatableRecordImpl<ContainerRecord> 
        implements Record6<String, String, String, String, String, Integer> {
// database, name, original_name, last_did
	private static final long serialVersionUID = -2107968478;

    /**
     * Setter for <code>torodb.container.database</code>.
     */
    public void setDatabase(String value) {
        setValue(0, value);
    }

    /**
     * Getter for <code>torodb.container.database</code>.
     */
    public String getDatabase() {
        return (String) getValue(0);
    }

    /**
     * Setter for <code>torodb.container.collection</code>.
     */
    public void setCollection(String value) {
        setValue(1, value);
    }

    /**
     * Getter for <code>torodb.container.collection</code>.
     */
    public String getCollection() {
        return (String) getValue(1);
    }

    /**
     * Setter for <code>torodb.container.path</code>.
     */
    public void setPath(String value) {
        setValue(2, value);
    }

    /**
     * Getter for <code>torodb.container.path</code>.
     */
    public String getPath() {
        return (String) getValue(2);
    }

    /**
     * Setter for <code>torodb.container.table_name</code>.
     */
    public void setTableName(String value) {
        setValue(3, value);
    }

    /**
     * Getter for <code>torodb.container.table_name</code>.
     */
    public String getTableName() {
        return (String) getValue(3);
    }

    /**
     * Setter for <code>torodb.container.table_name</code>.
     */
    public void setParentTableName(String value) {
        setValue(4, value);
    }

    /**
     * Getter for <code>torodb.container.table_name</code>.
     */
    public String getParentTableName() {
        return (String) getValue(4);
    }

    /**
     * Setter for <code>torodb.container.last_rid</code>.
     */
    public void setLastRid(Integer value) {
        setValue(5, value);
    }

    /**
     * Getter for <code>torodb.container.last_rid</code>.
     */
    public Integer getLastRid() {
        return (Integer) getValue(5);
    }

	// -------------------------------------------------------------------------
	// Primary key information
	// -------------------------------------------------------------------------

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Record3<String, String, String> key() {
		return (Record3) super.key();
	}

	// -------------------------------------------------------------------------
	// Record7 type implementation
	// -------------------------------------------------------------------------

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Row6<String, String, String, String, String, Integer> fieldsRow() {
		return (Row6) super.fieldsRow();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Row6<String, String, String, String, String, Integer> valuesRow() {
		return (Row6) super.valuesRow();
	}

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<String> field1() {
        return containerTable.DATABASE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<String> field2() {
        return containerTable.COLLECTION;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<String> field3() {
        return containerTable.PATH;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Field<String> field4() {
		return containerTable.TABLE;
	}

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<String> field5() {
        return containerTable.PARENT_TABLE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<Integer> field6() {
        return containerTable.LAST_RID;
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
        return getTableName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String value5() {
        return getParentTableName();
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Integer value6() {
		return getLastRid();
	}

    /**
     * {@inheritDoc}
     */
    @Override
    public ContainerRecord value1(String value) {
        setDatabase(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ContainerRecord value2(String value) {
        setCollection(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ContainerRecord value3(String value) {
        setPath(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ContainerRecord value4(String value) {
        setTableName(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ContainerRecord value5(String value) {
        setParentTableName(value);
        return this;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ContainerRecord value6(Integer value) {
		setLastRid(value);
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ContainerRecord values(String value1, String value2, String value3, String value4, String value5, Integer value6) {
		return this;
	}

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    private final ContainerTable containerTable;
    
    /**
     * Create a detached ContainerRecord
     */
    public ContainerRecord(ContainerTable containerTable) {
        super(containerTable);
        
        this.containerTable = containerTable;
    }
}
