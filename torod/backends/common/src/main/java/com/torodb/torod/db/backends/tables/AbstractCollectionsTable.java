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
package com.torodb.torod.db.backends.tables;

import java.util.Arrays;
import java.util.List;

import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.UniqueKey;
import org.jooq.impl.AbstractKeys;
import org.jooq.impl.TableImpl;

import com.torodb.torod.db.backends.DatabaseInterface;
import com.torodb.torod.db.backends.meta.TorodbSchema;
import com.torodb.torod.db.backends.tables.records.AbstractCollectionsRecord;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings("EQ_DOESNT_OVERRIDE_EQUALS")
public abstract class AbstractCollectionsTable<Record extends AbstractCollectionsRecord> extends TableImpl<Record> {

	private static final long serialVersionUID = 740755688;

    public static final String TABLE_NAME = "collections";

    public enum TableFields {
        NAME            (   "name"              ),
        SCHEMA          (   "schema"            ),
        CAPPED          (   "capped"            ),
        MAX_SIZE        (   "max_size"          ),
        MAX_ELEMENTS    (   "max_elements"      ),
        OTHER           (   "other"             ),
        STORAGE_ENGINE  (   "storage_engine"    )
        ;

        public final String fieldName;

        TableFields(String fieldName) {
            this.fieldName = fieldName;
        }

        @Override
        public String toString() {
            return fieldName;
        }
    }

	/**
	 * The class holding records for this type
     * @return 
	 */
	@Override
	public abstract Class<Record> getRecordType();

	/**
	 * The column <code>torodb.collections.name</code>.
	 */
	public final TableField<Record, String> NAME 
            = createNameField();

	/**
	 * The column <code>torodb.collections.schema</code>.
	 */
	public final TableField<Record, String> SCHEMA 
            = createSchemaField();

	/**
	 * The column <code>torodb.collections.capped</code>.
	 */
	public final TableField<Record, Boolean> CAPPED 
            = createCappedField();

	/**
	 * The column <code>torodb.collections.max_size</code>.
	 */
	public final TableField<Record, Integer> MAX_SIZE 
            = createMaxSizeField();

	/**
	 * The column <code>torodb.collections.max_elementes</code>.
	 */
	public final TableField<Record, Integer> MAX_ELEMENTES 
            = createMaxElementsField();

	/**
	 * The column <code>torodb.collections.other</code>.
	 */
	public final TableField<Record, String> OTHER 
            = createOtherField();
	
    public final TableField<Record, String> STORAGE_ENGINE 
            = createStorageEngineField();

    protected abstract TableField<Record, String> createNameField();
    protected abstract TableField<Record, String> createSchemaField();
    protected abstract TableField<Record, Boolean> createCappedField();
    protected abstract TableField<Record, Integer> createMaxSizeField();
    protected abstract TableField<Record, Integer> createMaxElementsField();
    protected abstract TableField<Record, String> createOtherField();
    protected abstract TableField<Record, String> createStorageEngineField();
    
    private final UniqueKeys<Record> uniqueKeys;
    
	/**
	 * Create a <code>torodb.collections</code> table reference
	 */
	public AbstractCollectionsTable() {
		this(TABLE_NAME, null);
	}

	protected AbstractCollectionsTable(String alias, Table<Record> aliased) {
		this(alias, aliased, null);
	}

	protected AbstractCollectionsTable(String alias, Table<Record> aliased, Field<?>[] parameters) {
		super(alias, TorodbSchema.TORODB, aliased, parameters, "");
		
		this.uniqueKeys = new UniqueKeys<>(this);
	}
    
    public String getSQLCreationStatement(DatabaseInterface databaseInterface) {
        return databaseInterface.createCollectionsTableStatement(getSchema().getName(), getName());
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public UniqueKey<Record> getPrimaryKey() {
		return uniqueKeys.COLLECTIONS_PKEY;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<UniqueKey<Record>> getKeys() {
		return Arrays.<UniqueKey<Record>>asList(uniqueKeys.COLLECTIONS_PKEY, 
		        uniqueKeys.COLLECTIONS_SCHEMA_KEY
        );
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public abstract AbstractCollectionsTable<Record> as(String alias);

	/**
	 * Rename this table
	 */
	public abstract AbstractCollectionsTable<Record> rename(String name);

    public boolean isSemanticallyEquals(Table<Record> table) {
        if (!table.getName().equals(getName())) {
            return false;
        }
        if (table.getSchema() == null || !getSchema().getName().equals(table.getSchema().getName())) {
            return false;
        }
        if (table.fields().length != 7) {
            return false;
        }
        return true; //TODO: improve the check
    }
    
    public UniqueKeys<Record> getUniqueKeys() {
        return uniqueKeys;
    }
    
    public static class UniqueKeys<KeyRecord extends AbstractCollectionsRecord> extends AbstractKeys {
        private final UniqueKey<KeyRecord> COLLECTIONS_PKEY;
        private final UniqueKey<KeyRecord> COLLECTIONS_SCHEMA_KEY;
        
        private UniqueKeys(AbstractCollectionsTable<KeyRecord> collectionsTable) {
            COLLECTIONS_PKEY = createUniqueKey(collectionsTable, collectionsTable.NAME);
            COLLECTIONS_SCHEMA_KEY = createUniqueKey(collectionsTable, collectionsTable.SCHEMA);
        }
    }
}
