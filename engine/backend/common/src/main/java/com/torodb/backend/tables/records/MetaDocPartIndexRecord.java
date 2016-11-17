/*
 * ToroDB - ToroDB: Backend common
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
import org.jooq.Record2;
import org.jooq.Record5;
import org.jooq.Row5;
import org.jooq.impl.UpdatableRecordImpl;

import com.torodb.backend.tables.MetaDocPartIndexTable;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;

public abstract class MetaDocPartIndexRecord<TableRefType> extends UpdatableRecordImpl<MetaDocPartIndexRecord<TableRefType>> 
        implements Record5<String, String, String, TableRefType, Boolean> {

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
     * Setter for <code>torodb.container.identifier</code>.
     */
    public void setIdentifier(String value) {
        set(1, value);
    }

    /**
     * Getter for <code>torodb.container.identifier</code>.
     */
    public String getIdentifier() {
        return (String) getValue(1);
    }

    /**
     * Setter for <code>torodb.container.collection</code>.
     */
    public void setCollection(String value) {
        set(2, value);
    }

    /**
     * Getter for <code>torodb.container.collection</code>.
     */
    public String getCollection() {
        return (String) getValue(2);
    }

    /**
     * Setter for <code>torodb.container.tableRef</code>.
     */
    public void setTableRef(TableRefType value) {
        set(3, value);
    }

    /**
     * Getter for <code>torodb.container.tableRef</code>.
     */
    @SuppressWarnings("unchecked")
    public TableRefType getTableRef() {
        return (TableRefType) getValue(3);
    }

    /**
     * Setter for <code>torodb.container.unique</code>.
     */
    public void setUnique(Boolean value) {
        set(4, value);
    }

    /**
     * Getter for <code>torodb.container.unique</code>.
     */
    public Boolean getUnique() {
        return (Boolean) getValue(4);
    }

	// -------------------------------------------------------------------------
	// Primary key information
	// -------------------------------------------------------------------------

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
    @Override
	public Record2<String, String> key() {
		return (Record2<String, String>) super.key();
	}

	// -------------------------------------------------------------------------
	// Record7 type implementation
	// -------------------------------------------------------------------------

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
    @Override
	public Row5<String, String, String, TableRefType, Boolean> fieldsRow() {
		return (Row5<String, String, String, TableRefType, Boolean>) super.fieldsRow();
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
    @Override
	public Row5<String, String, String, TableRefType, Boolean> valuesRow() {
		return (Row5<String, String, String, TableRefType, Boolean>) super.valuesRow();
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
        return metaDocPartTable.IDENTIFIER;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<String> field3() {
        return metaDocPartTable.COLLECTION;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<TableRefType> field4() {
        return metaDocPartTable.TABLE_REF;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<Boolean> field5() {
        return metaDocPartTable.UNIQUE;
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
        return getIdentifier();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String value3() {
        return getCollection();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableRefType value4() {
        return getTableRef();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Boolean value5() {
        return getUnique();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaDocPartIndexRecord<TableRefType> value1(String value) {
        setDatabase(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaDocPartIndexRecord<TableRefType> value2(String value) {
        setIdentifier(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaDocPartIndexRecord<TableRefType> value3(String value) {
        setCollection(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaDocPartIndexRecord<TableRefType> value4(TableRefType value) {
        setTableRef(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaDocPartIndexRecord<TableRefType> value5(Boolean value) {
        setUnique(value);
        return this;
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public abstract MetaDocPartIndexRecord<TableRefType> values(String database, String identifier, String collection, TableRefType tableRef, Boolean unique);

    public MetaDocPartIndexRecord<TableRefType> values(String database, String identifier, String collection, TableRef tableRef, Boolean unique) {
        return values(database, identifier, collection, toTableRefType(tableRef), unique);
    }
    
    protected abstract TableRefType toTableRefType(TableRef tableRef);
    
    public abstract TableRef getTableRefValue(TableRefFactory tableRefFactory);
	
    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    private final MetaDocPartIndexTable<TableRefType, MetaDocPartIndexRecord<TableRefType>> metaDocPartTable;
    
    /**
     * Create a detached MetaDocPartRecord
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public MetaDocPartIndexRecord(MetaDocPartIndexTable metaDocPartTable) {
        super(metaDocPartTable);
        
        this.metaDocPartTable = metaDocPartTable;
    }
}
