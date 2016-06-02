package com.torodb.backend.tables;

import java.util.Arrays;
import java.util.List;

import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.UniqueKey;
import org.jooq.impl.AbstractKeys;

import com.torodb.backend.DatabaseInterface;
import com.torodb.backend.meta.TorodbSchema;
import com.torodb.backend.tables.records.MetaCollectionRecord;

public abstract class MetaCollectionTable<R extends MetaCollectionRecord> extends SemanticTableImpl<R> {

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
    public abstract Class<R> getRecordType();

    /**
     * The column <code>torodb.collection.database</code>.
     */
    public final TableField<R, String> DATABASE 
            = createDatabaseField();

    /**
     * The column <code>torodb.collection.name</code>.
     */
    public final TableField<R, String> NAME 
            = createNameField();

    protected abstract TableField<R, String> createDatabaseField();
    protected abstract TableField<R, String> createNameField();

    private final UniqueKeys<R> uniqueKeys;
    
    /**
     * Create a <code>torodb.collections</code> table reference
     */
    public MetaCollectionTable() {
        this(TABLE_NAME, null);
    }

    protected MetaCollectionTable(String alias, Table<R> aliased) {
        this(alias, aliased, null);
    }

    protected MetaCollectionTable(String alias, Table<R> aliased, Field<?>[] parameters) {
        super(alias, TorodbSchema.TORODB, aliased, parameters, "");
        
        this.uniqueKeys = new UniqueKeys<R>(this);
    }
    
    public String getSQLCreationStatement(DatabaseInterface databaseInterface) {
        return databaseInterface.createMetaCollectionTableStatement(getSchema().getName(), getName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UniqueKey<R> getPrimaryKey() {
        return uniqueKeys.COLLECTION_PKEY;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<UniqueKey<R>> getKeys() {
        return Arrays.<UniqueKey<R>>asList(uniqueKeys.COLLECTION_PKEY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract MetaCollectionTable<R> as(String alias);

    /**
     * Rename this table
     */
    public abstract MetaCollectionTable<R> rename(String name);
    
    public UniqueKeys<R> getUniqueKeys() {
        return uniqueKeys;
    }
    
    public static class UniqueKeys<KeyRecord extends MetaCollectionRecord> extends AbstractKeys {
        private final UniqueKey<KeyRecord> COLLECTION_PKEY;
        
        private UniqueKeys(MetaCollectionTable<KeyRecord> collectionsTable) {
            COLLECTION_PKEY = createUniqueKey(collectionsTable, collectionsTable.DATABASE, collectionsTable.NAME);
        }
    }
}
