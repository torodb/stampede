/*
 * MongoWP - ToroDB-poc: Backend common
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
package com.torodb.backend.tables.records;

import org.jooq.Field;
import org.jooq.Record3;
import org.jooq.Record5;
import org.jooq.Row5;
import org.jooq.impl.UpdatableRecordImpl;

import com.torodb.backend.tables.MetaDocPartTable;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;

public abstract class MetaDocPartRecord<TableRefType> extends UpdatableRecordImpl<MetaDocPartRecord<TableRefType>> 
        implements Record5<String, String, TableRefType, String, Integer> {

    private static final long serialVersionUID = -2107968478;

    /**
     * Setter for <code>torodb.container.database</code>.
     */
    public void setDatabase(String value) {
        set(0, value);
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
        set(1, value);
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
        set(2, value);
    }

    /**
     * Getter for <code>torodb.container.tableRef</code>.
     */
    @SuppressWarnings("unchecked")
    public TableRefType getTableRef() {
        return (TableRefType) getValue(2);
    }

    /**
     * Setter for <code>torodb.container.identifier</code>.
     */
    public void setIdentifier(String value) {
        set(3, value);
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
        set(4, value);
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
	@SuppressWarnings("unchecked")
    @Override
	public Record3<String, String, String> key() {
		return (Record3<String, String, String>) super.key();
	}

	// -------------------------------------------------------------------------
	// Record7 type implementation
	// -------------------------------------------------------------------------

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
    @Override
	public Row5<String, String, TableRefType, String, Integer> fieldsRow() {
		return (Row5<String, String, TableRefType, String, Integer>) super.fieldsRow();
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
    @Override
	public Row5<String, String, TableRefType, String, Integer> valuesRow() {
		return (Row5<String, String, TableRefType, String, Integer>) super.valuesRow();
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
    public MetaDocPartRecord<TableRefType> value1(String value) {
        setDatabase(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaDocPartRecord<TableRefType> value2(String value) {
        setCollection(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaDocPartRecord<TableRefType> value3(TableRefType value) {
        setTableRef(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaDocPartRecord<TableRefType> value4(String value) {
        setIdentifier(value);
        return this;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public MetaDocPartRecord<TableRefType> value5(Integer value) {
		setLastRid(value);
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public abstract MetaDocPartRecord<TableRefType> values(String database, String collection, TableRefType tableRef, String identifier, Integer lastRid);

    public MetaDocPartRecord<TableRefType> values(String database, String collection, TableRef tableRef, String identifier) {
        return values(database, collection, toTableRefType(tableRef), identifier, 0);
    }
    
    protected abstract TableRefType toTableRefType(TableRef tableRef);
    
    public abstract TableRef getTableRefValue(TableRefFactory tableRefFactory);
	
    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    private final MetaDocPartTable<TableRefType, MetaDocPartRecord<TableRefType>> metaDocPartTable;
    
    /**
     * Create a detached MetaDocPartRecord
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public MetaDocPartRecord(MetaDocPartTable metaDocPartTable) {
        super(metaDocPartTable);
        
        this.metaDocPartTable = metaDocPartTable;
    }
}
