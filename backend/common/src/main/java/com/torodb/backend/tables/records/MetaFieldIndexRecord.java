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
import org.jooq.Record4;
import org.jooq.Record8;
import org.jooq.Row8;
import org.jooq.impl.UpdatableRecordImpl;

import com.torodb.backend.tables.MetaFieldIndexTable;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.transaction.metainf.FieldIndexOrdering;
import com.torodb.core.transaction.metainf.FieldType;

public abstract class MetaFieldIndexRecord<TableRefType> extends UpdatableRecordImpl<MetaFieldIndexRecord<TableRefType>> 
        implements Record8<String, String, Integer, String, TableRefType, String, FieldType, FieldIndexOrdering> {

    private static final long serialVersionUID = 4696260394225350907L;

    /**
     * Setter for <code>torodb.field_index.database</code>.
     */
    public void setDatabase(String value) {
        set(0, value);
    }

    /**
     * Getter for <code>torodb.field_index.database</code>.
     */
    public String getDatabase() {
        return (String) getValue(0);
    }

    /**
     * Setter for <code>torodb.field_index.identifier</code>.
     */
    public void setIdentifier(String value) {
        set(1, value);
    }

    /**
     * Getter for <code>torodb.field_index.identifier</code>.
     */
    public String getIdentifier() {
        return (String) getValue(1);
    }

    /**
     * Setter for <code>torodb.field_index.position</code>.
     */
    public void setPosition(Integer value) {
        set(2, value);
    }

    /**
     * Getter for <code>torodb.field_index.position</code>.
     */
    public Integer getPosition() {
        return (Integer) getValue(2);
    }

    /**
     * Setter for <code>torodb.field_index.collection</code>.
     */
    public void setCollection(String value) {
        set(3, value);
    }

    /**
     * Getter for <code>torodb.field_index.collection</code>.
     */
    public String getCollection() {
        return (String) getValue(3);
    }

    /**
     * Setter for <code>torodb.field_index.tableRef</code>.
     */
    public void setTableRef(TableRefType value) {
        set(4, value);
    }

    /**
     * Getter for <code>torodb.field_index.tableRef</code>.
     */
    @SuppressWarnings("unchecked")
    public TableRefType getTableRef() {
        return (TableRefType) getValue(4);
    }

    /**
     * Setter for <code>torodb.field_index.name</code>.
     */
    public void setName(String value) {
        set(5, value);
    }

    /**
     * Getter for <code>torodb.field_index.name</code>.
     */
    public String getName() {
        return (String) getValue(5);
    }

    /**
     * Setter for <code>torodb.field_index.type</code>.
     */
    public void setType(FieldType value) {
        set(6, value);
    }

    /**
     * Getter for <code>torodb.field_index.type</code>.
     */
    public FieldType getType() {
        return (FieldType) getValue(6);
    }

    /**
     * Setter for <code>torodb.field_index.ordering</code>.
     */
    public void setOrdering(FieldIndexOrdering value) {
        set(7, value);
    }

    /**
     * Getter for <code>torodb.field_index.ordering</code>.
     */
    public FieldIndexOrdering getOrdering() {
        return (FieldIndexOrdering) getValue(7);
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
	public Row8<String, String, Integer, String, TableRefType, String, FieldType, FieldIndexOrdering> fieldsRow() {
		return (Row8<String, String, Integer, String, TableRefType, String, FieldType, FieldIndexOrdering>) super.fieldsRow();
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
    @Override
	public Row8<String, String, Integer, String, TableRefType, String, FieldType, FieldIndexOrdering> valuesRow() {
		return (Row8<String, String, Integer, String, TableRefType, String, FieldType, FieldIndexOrdering>) super.valuesRow();
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
        return metaIndexFieldTable.IDENTIFIER;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<Integer> field3() {
        return metaIndexFieldTable.POSITION;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<String> field4() {
        return metaIndexFieldTable.COLLECTION;
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
    public Field<FieldType> field7() {
        return metaIndexFieldTable.TYPE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<FieldIndexOrdering> field8() {
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
        return getIdentifier();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Integer value3() {
        return getPosition();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String value4() {
        return getCollection();
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
    public FieldType value7() {
        return getType();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FieldIndexOrdering value8() {
        return getOrdering();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaFieldIndexRecord<TableRefType> value1(String value) {
        setDatabase(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaFieldIndexRecord<TableRefType> value2(String value) {
        setIdentifier(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaFieldIndexRecord<TableRefType> value3(Integer value) {
        setPosition(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaFieldIndexRecord<TableRefType> value4(String value) {
        setCollection(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaFieldIndexRecord<TableRefType> value5(TableRefType value) {
        setTableRef(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaFieldIndexRecord<TableRefType> value6(String value) {
        setName(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaFieldIndexRecord<TableRefType> value7(FieldType value) {
        setType(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaFieldIndexRecord<TableRefType> value8(FieldIndexOrdering value) {
        setOrdering(value);
        return this;
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public abstract MetaFieldIndexRecord<TableRefType> values(String database, String identifier, Integer position, String collection, TableRefType tableRef, String name, FieldType type, FieldIndexOrdering fieldIndexOrdering);

    public MetaFieldIndexRecord<TableRefType> values(String database, String identifier, Integer position, String collection, TableRef tableRef, String name, FieldType type, FieldIndexOrdering fieldIndexOrdering) {
        return values(database, identifier, position, collection, toTableRefType(tableRef), name, type, fieldIndexOrdering);
    }
    
    protected abstract TableRefType toTableRefType(TableRef tableRef);
    
    public abstract TableRef getTableRefValue(TableRefFactory tableRefFactory);

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    private final MetaFieldIndexTable<TableRefType, MetaFieldIndexRecord<TableRefType>> metaIndexFieldTable;
    
    /**
     * Create a detached MetaFieldRecord
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public MetaFieldIndexRecord(MetaFieldIndexTable metaFieldTable) {
        super(metaFieldTable);
        
        this.metaIndexFieldTable = metaFieldTable;
    }
}