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
package com.torodb.backend.tables.records;

import org.jooq.Field;
import org.jooq.Record3;
import org.jooq.Record5;
import org.jooq.Row5;
import org.jooq.impl.UpdatableRecordImpl;

import com.torodb.backend.tables.MetaDocPartTable;

public abstract class MetaDocPartRecord<TableRefType> extends UpdatableRecordImpl<MetaDocPartRecord<TableRefType>> 
        implements Record5<String, String, TableRefType, String, Integer> {
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
     * Setter for <code>torodb.container.tableRef</code>.
     */
    public void setTableRef(TableRefType value) {
        setValue(2, value);
    }

    /**
     * Getter for <code>torodb.container.tableRef</code>.
     */
    public TableRefType getTableRef() {
        return (TableRefType) getValue(2);
    }

    /**
     * Setter for <code>torodb.container.identifier</code>.
     */
    public void setIdentifier(String value) {
        setValue(3, value);
    }

    /**
     * Getter for <code>torodb.container.identifier</code>.
     */
    public String getIdentifier() {
        return (String) getValue(3);
    }

    /**
     * Setter for <code>torodb.container.last_rid</code>.
     */
    public void setLastRid(Integer value) {
        setValue(4, value);
    }

    /**
     * Getter for <code>torodb.container.last_rid</code>.
     */
    public Integer getLastRid() {
        return (Integer) getValue(4);
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
	public Row5<String, String, TableRefType, String, Integer> fieldsRow() {
		return (Row5) super.fieldsRow();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Row5<String, String, TableRefType, String, Integer> valuesRow() {
		return (Row5) super.valuesRow();
	}

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<String> field1() {
        return metaDocPartTable.DATABASE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<String> field2() {
        return metaDocPartTable.COLLECTION;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<TableRefType> field3() {
        return metaDocPartTable.TABLE_REF;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Field<String> field4() {
		return metaDocPartTable.IDENTIFIER;
	}

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<Integer> field5() {
        return metaDocPartTable.LAST_RID;
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
    public TableRefType value3() {
        return getTableRef();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String value4() {
        return getIdentifier();
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Integer value5() {
		return getLastRid();
	}

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaDocPartRecord value1(String value) {
        setDatabase(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaDocPartRecord value2(String value) {
        setCollection(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaDocPartRecord value3(TableRefType value) {
        setTableRef(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaDocPartRecord value4(String value) {
        setIdentifier(value);
        return this;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public MetaDocPartRecord value5(Integer value) {
		setLastRid(value);
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public abstract MetaDocPartRecord values(String value1, String value2, TableRefType value3, String value4, Integer value5);

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    private final MetaDocPartTable metaDocPartTable;
    
    /**
     * Create a detached MetaDocPartRecord
     */
    public MetaDocPartRecord(MetaDocPartTable metaDocPartTable) {
        super(metaDocPartTable);
        
        this.metaDocPartTable = metaDocPartTable;
    }
}
