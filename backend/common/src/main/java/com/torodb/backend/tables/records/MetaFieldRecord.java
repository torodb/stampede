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
import org.jooq.Record6;
import org.jooq.Row6;
import org.jooq.impl.UpdatableRecordImpl;

import com.torodb.backend.tables.MetaFieldTable;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.transaction.metainf.FieldType;

public abstract class MetaFieldRecord<TableRefType> extends UpdatableRecordImpl<MetaFieldRecord<TableRefType>> 
        implements Record6<String, String, TableRefType, String, String, FieldType> {
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
     * Setter for <code>torodb.field.tableRef</code>.
     */
    public void setTableRef(TableRefType value) {
        setValue(2, value);
    }

    /**
     * Getter for <code>torodb.field.tableRef</code>.
     */
    public TableRefType getTableRef() {
        return (TableRefType) getValue(2);
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
     * Setter for <code>torodb.field.idenftifier</code>.
     */
    public void setIdentifier(String value) {
        setValue(4, value);
    }

    /**
     * Getter for <code>torodb.field.idenftifier</code>.
     */
    public String getIdentifier() {
        return (String) getValue(4);
    }

    /**
     * Setter for <code>torodb.field.type</code>.
     */
    public void setType(FieldType value) {
        setValue(5, value);
    }

    /**
     * Getter for <code>torodb.field.type</code>.
     */
    public FieldType getType() {
        return (FieldType) getValue(5);
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
	public Row6<String, String, TableRefType, String, String, FieldType> fieldsRow() {
		return (Row6) super.fieldsRow();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Row6<String, String, TableRefType, String, String, FieldType> valuesRow() {
		return (Row6) super.valuesRow();
	}

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<String> field1() {
        return metaFieldTable.DATABASE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<String> field2() {
        return metaFieldTable.COLLECTION;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<TableRefType> field3() {
        return metaFieldTable.TABLE_REF;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<String> field4() {
        return metaFieldTable.NAME;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<String> field5() {
        return metaFieldTable.IDENTIFIER;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Field<FieldType> field6() {
        return metaFieldTable.TYPE;
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
        return getName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String value5() {
        return getIdentifier();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FieldType value6() {
        return getType();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaFieldRecord value1(String value) {
        setDatabase(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaFieldRecord value2(String value) {
        setCollection(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaFieldRecord value3(TableRefType value) {
        setTableRef(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaFieldRecord value4(String value) {
        setName(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaFieldRecord value5(String value) {
        setIdentifier(value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MetaFieldRecord value6(FieldType value) {
        setType(value);
        return this;
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public abstract MetaFieldRecord values(String database, String collection, TableRefType tableRef, String name, String identifier, FieldType type);

    public MetaFieldRecord values(String database, String collection, TableRef tableRef, String name, String identifier, FieldType type) {
        return values(database, collection, toTableRefType(tableRef), name, identifier, type);
    }
    
    protected abstract TableRefType toTableRefType(TableRef tableRef);
    
    public abstract TableRef getTableRefValue(TableRefFactory tableRefFactory);

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    private final MetaFieldTable metaFieldTable;
    
    /**
     * Create a detached MetaFieldRecord
     */
    public MetaFieldRecord(MetaFieldTable metaFieldTable) {
        super(metaFieldTable);
        
        this.metaFieldTable = metaFieldTable;
    }
}
