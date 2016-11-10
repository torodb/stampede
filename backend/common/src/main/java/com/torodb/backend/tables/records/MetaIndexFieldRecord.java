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
import org.jooq.Record4;
import org.jooq.Record7;
import org.jooq.Row7;
import org.jooq.impl.UpdatableRecordImpl;

import com.torodb.backend.tables.MetaIndexFieldTable;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.transaction.metainf.FieldIndexOrdering;

public abstract class MetaIndexFieldRecord<TableRefType> extends UpdatableRecordImpl<MetaIndexFieldRecord<TableRefType>> 
        implements Record7<String, String, String, Integer, TableRefType, String, FieldIndexOrdering> {

    private static final long serialVersionUID = 4696260394225350907L;

    /**
     * Setter for <code>torodb.field.database</code>.
     */
    public void setDatabase(String value) {
        set(0, value);
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
        set(1, value);
    }

    /**
     * Getter for <code>torodb.field.collection</code>.
     */
    public String getCollection() {
        return (String) getValue(1);
    }

    /**
     * Setter for <code>torodb.field.index</code>.
     */
    public void setIndex(String value) {
        set(2, value);
    }

    /**
     * Getter for <code>torodb.field.index</code>.
     */
    public String getIndex() {
        return (String) getValue(2);
    }

    /**
     * Setter for <code>torodb.field.position</code>.
     */
    public void setPosition(Integer value) {
        set(3, value);
    }

    /**
     * Getter for <code>torodb.field.position</code>.
     */
    public Integer getPosition() {
        return (Integer) getValue(3);
    }

    /**
     * Setter for <code>torodb.field.tableRef</code>.
     */
    public void setTableRef(TableRefType value) {
        set(4, value);
    }

    /**
     * Getter for <code>torodb.field.tableRef</code>.
     */
    @SuppressWarnings("unchecked")
    public TableRefType getTableRef() {
        return (TableRefType) getValue(4);
    }

    /**
     * Setter for <code>torodb.field.name</code>.
     */
    public void setName(String value) {
        set(5, value);
    }

    /**
     * Getter for <code>torodb.field.name</code>.
     */
    public String getName() {
        return (String) getValue(5);
    }

    /**
     * Setter for <code>torodb.field.ordering</code>.
     */
    public void setOrdering(FieldIndexOrdering value) {
        set(6, value);
    }

    /**
     * Getter for <code>torodb.field.ordering</code>.
     */
    public FieldIndexOrdering getOrdering() {
        return (FieldIndexOrdering) getValue(6);
    }

	// -------------------------------------------------------------------------
	// Primary key information
	// -------------------------------------------------------------------------

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
    @Override
	public Record4<String, String, String, Integer> key() {
		return (Record4<String, String, String, Integer>) super.key();
	}

	// -------------------------------------------------------------------------
	// Record7 type implementation
	// -------------------------------------------------------------------------

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
    @Override
	public Row7<String, String, String, Integer, TableRefType, String, FieldIndexOrdering> fieldsRow() {
		return (Row7<String, String, String, Integer, TableRefType, String, FieldIndexOrdering>) super.fieldsRow();
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
    @Override
	public Row7<String, String, String, Integer, TableRefType, String, FieldIndexOrdering> valuesRow() {
		return (Row7<String, String, String, Integer, TableRefType, String, FieldIndexOrdering>) super.valuesRow();
	}

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<String> field1() {
        return metaIndexFieldTable.DATABASE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<String> field2() {
        return metaIndexFieldTable.COLLECTION;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<String> field3() {
        return metaIndexFieldTable.INDEX;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<Integer> field4() {
        return metaIndexFieldTable.POSITION;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<TableRefType> field5() {
        return metaIndexFieldTable.TABLE_REF;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<String> field6() {
        return metaIndexFieldTable.NAME;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<FieldIndexOrdering> field7() {
        return metaIndexFieldTable.ORDERING;
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
        return getIndex();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Integer value4() {
        return getPosition();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableRefType value5() {
        return getTableRef();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String value6() {
        return getName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FieldIndexOrdering value7() {
        return getOrdering();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaIndexFieldRecord<TableRefType> value1(String value) {
        setDatabase(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaIndexFieldRecord<TableRefType> value2(String value) {
        setCollection(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaIndexFieldRecord<TableRefType> value3(String value) {
        setIndex(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaIndexFieldRecord<TableRefType> value4(Integer value) {
        setPosition(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaIndexFieldRecord<TableRefType> value5(TableRefType value) {
        setTableRef(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaIndexFieldRecord<TableRefType> value6(String value) {
        setName(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaIndexFieldRecord<TableRefType> value7(FieldIndexOrdering value) {
        setOrdering(value);
        return this;
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public abstract MetaIndexFieldRecord<TableRefType> values(String database, String collection, String index, Integer position, TableRefType tableRef, String name, FieldIndexOrdering fieldIndexOrdering);

    public MetaIndexFieldRecord<TableRefType> values(String database, String collection, String index, Integer position, TableRef tableRef, String name, FieldIndexOrdering fieldIndexOrdering) {
        return values(database, collection, index, position, toTableRefType(tableRef), name, fieldIndexOrdering);
    }
    
    protected abstract TableRefType toTableRefType(TableRef tableRef);
    
    public abstract TableRef getTableRefValue(TableRefFactory tableRefFactory);

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    private final MetaIndexFieldTable<TableRefType, MetaIndexFieldRecord<TableRefType>> metaIndexFieldTable;
    
    /**
     * Create a detached MetaFieldRecord
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public MetaIndexFieldRecord(MetaIndexFieldTable metaFieldTable) {
        super(metaFieldTable);
        
        this.metaIndexFieldTable = metaFieldTable;
    }
}
