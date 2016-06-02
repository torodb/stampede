package com.torodb.backend.tables;

import java.util.Arrays;
import java.util.List;

import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.UniqueKey;
import org.jooq.impl.AbstractKeys;
import org.jooq.impl.TableImpl;

import com.torodb.backend.DatabaseInterface;
import com.torodb.backend.meta.TorodbSchema;
import com.torodb.backend.tables.records.MetaCollectionRecord;

public abstract class MetaCollectionTable<Record extends MetaCollectionRecord> extends TableImpl<Record> {

    private static final long serialVersionUID = 740755688;

    public static final String TABLE_NAME = "collection";

    public enum TableFields {
        DATABASE        (   "database"          ),
        NAME            (   "name"              ),
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

    protected abstract TableField<Record, String> createDatabaseField();
    protected abstract TableField<Record, String> createNameField();

    private final UniqueKeys<Record> uniqueKeys;
    
    /**
     * Create a <code>torodb.collections</code> table reference
     */
    public MetaCollectionTable() {
        this(TABLE_NAME, null);
    }

    protected MetaCollectionTable(String alias, Table<Record> aliased) {
        this(alias, aliased, null);
    }

    protected MetaCollectionTable(String alias, Table<Record> aliased, Field<?>[] parameters) {
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
        return Arrays.<UniqueKey<Record>>asList(uniqueKeys.COLLECTION_PKEY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract MetaCollectionTable<Record> as(String alias);

    /**
     * Rename this table
     */
    public abstract MetaCollectionTable<Record> rename(String name);

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
    
    public static class UniqueKeys<KeyRecord extends MetaCollectionRecord> extends AbstractKeys {
        private final UniqueKey<KeyRecord> COLLECTION_PKEY;
        
        private UniqueKeys(MetaCollectionTable<KeyRecord> collectionsTable) {
            COLLECTION_PKEY = createUniqueKey(collectionsTable, collectionsTable.DATABASE, collectionsTable.NAME);
        }
    }
}
