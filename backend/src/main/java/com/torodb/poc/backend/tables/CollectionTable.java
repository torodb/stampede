package com.torodb.poc.backend.tables;

import java.util.Arrays;
import java.util.List;

import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.UniqueKey;
import org.jooq.impl.AbstractKeys;
import org.jooq.impl.TableImpl;

import com.torodb.poc.backend.DatabaseInterface;
import com.torodb.poc.backend.meta.TorodbSchema;
import com.torodb.poc.backend.tables.records.CollectionRecord;

public abstract class CollectionTable<Record extends CollectionRecord> extends TableImpl<Record> {

    private static final long serialVersionUID = 740755688;

    public static final String TABLE_NAME = "collection";

    public enum TableFields {
        DATABASE        (   "database"          ),
        NAME            (   "name"              ),
        TABLE_NAME      (   "table_name"        ),
        CAPPED          (   "capped"            ),
        MAX_SIZE        (   "max_size"          ),
        MAX_ELEMENTS    (   "max_elements"      ),
        OTHER           (   "other"             ),
        STORAGE_ENGINE  (   "storage_engine"    ),
        LAST_DID        (   "last_did"          )
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
     * The column <code>torodb.collection.database</code>.
     */
    public final TableField<Record, String> DATABASE 
            = createDatabaseField();

    /**
     * The column <code>torodb.collection.name</code>.
     */
    public final TableField<Record, String> NAME 
            = createNameField();

    /**
     * The column <code>torodb.collection.table_name</code>.
     */
    public final TableField<Record, String> TABLE 
            = createTableNameField();

    /**
     * The column <code>torodb.collection.capped</code>.
     */
    public final TableField<Record, Boolean> CAPPED 
            = createCappedField();

    /**
     * The column <code>torodb.collection.max_size</code>.
     */
    public final TableField<Record, Integer> MAX_SIZE 
            = createMaxSizeField();

    /**
     * The column <code>torodb.collection.max_elementes</code>.
     */
    public final TableField<Record, Integer> MAX_ELEMENTES 
            = createMaxElementsField();

    /**
     * The column <code>torodb.collection.other</code>.
     */
    public final TableField<Record, String> OTHER 
            = createOtherField();
    
    /**
     * The column <code>torodb.collection.storage_engine</code>.
     */
    public final TableField<Record, String> STORAGE_ENGINE 
            = createStorageEngineField();

    /**
     * The column <code>torodb.collection.last_did</code>.
     */
    public final TableField<Record, Integer> LAST_DID 
            = createLastDidField();

    protected abstract TableField<Record, String> createDatabaseField();
    protected abstract TableField<Record, String> createNameField();
    protected abstract TableField<Record, String> createTableNameField();
    protected abstract TableField<Record, Boolean> createCappedField();
    protected abstract TableField<Record, Integer> createMaxSizeField();
    protected abstract TableField<Record, Integer> createMaxElementsField();
    protected abstract TableField<Record, String> createOtherField();
    protected abstract TableField<Record, String> createStorageEngineField();
    protected abstract TableField<Record, Integer> createLastDidField();

    private final UniqueKeys<Record> uniqueKeys;
    
    /**
     * Create a <code>torodb.collections</code> table reference
     */
    public CollectionTable() {
        this(TABLE_NAME, null);
    }

    protected CollectionTable(String alias, Table<Record> aliased) {
        this(alias, aliased, null);
    }

    protected CollectionTable(String alias, Table<Record> aliased, Field<?>[] parameters) {
        super(alias, TorodbSchema.TORODB, aliased, parameters, "");
        
        this.uniqueKeys = new UniqueKeys<Record>(this);
    }
    
    public String getSQLCreationStatement(DatabaseInterface databaseInterface) {
        return databaseInterface.createCollectionTableStatement(getSchema().getName(), getName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UniqueKey<Record> getPrimaryKey() {
        return uniqueKeys.COLLECTION_PKEY;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<UniqueKey<Record>> getKeys() {
        return Arrays.<UniqueKey<Record>>asList(uniqueKeys.COLLECTION_PKEY, uniqueKeys.COLLECTION_TABLE_NAME_UNIQUE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract CollectionTable<Record> as(String alias);

    /**
     * Rename this table
     */
    public abstract CollectionTable<Record> rename(String name);

    public boolean isSemanticallyEquals(Table<Record> table) {
        if (!table.getName().equals(getName())) {
            return false;
        }
        if (table.getSchema() == null || !getSchema().getName().equals(table.getSchema().getName())) {
            return false;
        }
        if (table.fields().length != 10) {
            return false;
        }
        return true; //TODO: improve the check
    }
    
    public UniqueKeys<Record> getUniqueKeys() {
        return uniqueKeys;
    }
    
    public static class UniqueKeys<KeyRecord extends CollectionRecord> extends AbstractKeys {
        private final UniqueKey<KeyRecord> COLLECTION_PKEY;
        private final UniqueKey<KeyRecord> COLLECTION_TABLE_NAME_UNIQUE;
        
        private UniqueKeys(CollectionTable<KeyRecord> collectionsTable) {
            COLLECTION_PKEY = createUniqueKey(collectionsTable, collectionsTable.DATABASE, collectionsTable.NAME);
            COLLECTION_TABLE_NAME_UNIQUE = createUniqueKey(collectionsTable, collectionsTable.DATABASE, collectionsTable.TABLE);
        }
    }
}
