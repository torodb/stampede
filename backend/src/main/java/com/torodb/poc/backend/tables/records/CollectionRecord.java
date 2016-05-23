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
import org.jooq.Record9;
import org.jooq.Row10;
import org.jooq.Row9;
import org.jooq.impl.UpdatableRecordImpl;

import com.torodb.poc.backend.tables.CollectionTable;

public abstract class CollectionRecord extends UpdatableRecordImpl<CollectionRecord> 
        implements Record9<String, String, String, Boolean, Integer, Integer, String, String, Integer> {
// database, name, original_name, last_did
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

    /**
     * Setter for <code>torodb.collection.table_name</code>.
     */
    public void setTableName(String value) {
        setValue(2, value);
    }

    /**
     * Getter for <code>torodb.collection.table_name</code>.
     */
    public String getTableName() {
        return (String) getValue(2);
    }

	/**
	 * Setter for <code>torodb.collection.capped</code>.
	 */
	public void setCapped(Boolean value) {
		setValue(3, value);
	}

	/**
	 * Getter for <code>torodb.collection.capped</code>.
	 */
	public Boolean isCapped() {
		return (Boolean) getValue(3);
	}

	/**
	 * Setter for <code>torodb.collection.max_size</code>.
	 */
	public void setMaxSize(Integer value) {
		setValue(4, value);
	}

	/**
	 * Getter for <code>torodb.collection.max_size</code>.
	 */
	public Integer getMaxSize() {
		return (Integer) getValue(4);
	}

	/**
	 * Setter for <code>torodb.collection.max_elementes</code>.
	 */
	public void setMaxElementes(Integer value) {
		setValue(5, value);
	}

	/**
	 * Getter for <code>torodb.collection.max_elementes</code>.
	 */
	public Integer getMaxElementes() {
		return (Integer) getValue(5);
	}

	/**
	 * Setter for <code>torodb.collection.other</code>.
	 */
	public void setOther(String value) {
		setValue(6, value);
	}

	/**
	 * Getter for <code>torodb.collection.other</code>.
	 */
	public String getOther() {
		return (String) getValue(6);
	}
    
    public void setStorageEngine(String engine) {
        setValue(7, engine);
    }
    
    public String getStorageEngine() {
        return (String) getValue(7);
    }

    /**
     * Setter for <code>torodb.collection.last_did</code>.
     */
    public void setLastDid(Integer value) {
        setValue(8, value);
    }

    /**
     * Getter for <code>torodb.collection.last_did</code>.
     */
    public Integer getLastDid() {
        return (Integer) getValue(8);
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
	public Row9<String, String, String, Boolean, Integer, Integer, String, String, Integer> fieldsRow() {
		return (Row9) super.fieldsRow();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Row9<String, String, String, Boolean, Integer, Integer, String, String, Integer> valuesRow() {
		return (Row9) super.valuesRow();
	}

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<String> field1() {
        return collectionTable.DATABASE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<String> field2() {
        return collectionTable.NAME;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<String> field3() {
        return collectionTable.TABLE;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Field<Boolean> field4() {
		return collectionTable.CAPPED;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Field<Integer> field5() {
		return collectionTable.MAX_SIZE;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Field<Integer> field6() {
		return collectionTable.MAX_ELEMENTES;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Field<String> field7() {
		return collectionTable.OTHER;
	}
    
    public Field<String> field8() {
        return collectionTable.STORAGE_ENGINE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<Integer> field9() {
        return collectionTable.LAST_DID;
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
    public String value3() {
        return getTableName();
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Boolean value4() {
		return isCapped();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Integer value5() {
		return getMaxSize();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Integer value6() {
		return getMaxElementes();
	}
    
	/**
	 * {@inheritDoc}
	 */
	@Override
	public String value7() {
		return getOther();
	}
    
    /**
     * {@inheritDoc}
     */
    @Override
	public String value8() {
		return getStorageEngine();
	}

    /**
     * {@inheritDoc}
     */
    @Override
    public Integer value9() {
        return getLastDid();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CollectionRecord value1(String value) {
        setName(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CollectionRecord value2(String value) {
        setName(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CollectionRecord value3(String value) {
        setTableName(value);
        return this;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public CollectionRecord value4(Boolean value) {
		setCapped(value);
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public CollectionRecord value5(Integer value) {
		setMaxSize(value);
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public CollectionRecord value6(Integer value) {
		setMaxElementes(value);
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public CollectionRecord value7(String value) {
		setOther(value);
		return this;
	}
    
    /**
     * {@inheritDoc}
     */
    public CollectionRecord value8(String value) {
		setStorageEngine(value);
		return this;
	}

    /**
     * {@inheritDoc}
     */
    @Override
    public CollectionRecord value9(Integer value) {
        setLastDid(value);
        return this;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public CollectionRecord values(String value1, String value2, String value3, Boolean value4, Integer value5, Integer value6, String value7, String value8, Integer value9) {
		return this;
	}

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    private final CollectionTable collectionTable;
    
    /**
     * Create a detached CollectionRecord
     */
    public CollectionRecord(CollectionTable collectionTable) {
        super(collectionTable);
        
        this.collectionTable = collectionTable;
    }
}
