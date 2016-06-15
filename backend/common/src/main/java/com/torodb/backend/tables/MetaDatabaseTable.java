package com.torodb.backend.tables;

import java.util.Arrays;
import java.util.List;

import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.UniqueKey;
import org.jooq.impl.AbstractKeys;

import com.torodb.backend.DatabaseInterface;
import com.torodb.backend.meta.TorodbSchema;
import com.torodb.backend.tables.records.MetaDatabaseRecord;

public abstract class MetaDatabaseTable<R extends MetaDatabaseRecord> extends SemanticTable<R> {

    private static final long serialVersionUID = -8840058751911188345L;

    public static final String TABLE_NAME = "database";

    public enum TableFields {
        NAME            (   "name"              ),
        IDENTIFIER      (   "identifier"        )
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
     * The column <code>torodb.database.name</code>.
     */
    public final TableField<R, String> NAME 
            = createNameField();

    /**
     * The column <code>torodb.database.schema</code>.
     */
    public final TableField<R, String> IDENTIFIER 
            = createIdentifierField();

    protected abstract TableField<R, String> createNameField();
    protected abstract TableField<R, String> createIdentifierField();
    
    private final UniqueKeys<R> uniqueKeys;
    
    /**
     * Create a <code>torodb.database</code> table reference
     */
    public MetaDatabaseTable() {
        this(TABLE_NAME, null);
    }

    protected MetaDatabaseTable(String alias, Table<R> aliased) {
        this(alias, aliased, null);
    }

    protected MetaDatabaseTable(String alias, Table<R> aliased, Field<?>[] parameters) {
        super(alias, TorodbSchema.TORODB, aliased, parameters, "");
        
        this.uniqueKeys = new UniqueKeys<R>(this);
    }
    
    public String getSQLCreationStatement(DatabaseInterface databaseInterface) {
        return databaseInterface.createMetaDatabaseTableStatement(getSchema().getName(), getName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UniqueKey<R> getPrimaryKey() {
        return uniqueKeys.DATABASE_PKEY;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<UniqueKey<R>> getKeys() {
        return Arrays.<UniqueKey<R>>asList(uniqueKeys.DATABASE_PKEY, 
                uniqueKeys.DATABASE_SCHEMA_UNIQUE
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract MetaDatabaseTable<R> as(String alias);

    /**
     * Rename this table
     */
    public abstract MetaDatabaseTable<R> rename(String name);
    
    public UniqueKeys<R> getUniqueKeys() {
        return uniqueKeys;
    }
    
    public static class UniqueKeys<KeyRecord extends MetaDatabaseRecord> extends AbstractKeys {
        private final UniqueKey<KeyRecord> DATABASE_PKEY;
        private final UniqueKey<KeyRecord> DATABASE_SCHEMA_UNIQUE;
        
        private UniqueKeys(MetaDatabaseTable<KeyRecord> databaseTable) {
            DATABASE_PKEY = createUniqueKey(databaseTable, databaseTable.NAME);
            DATABASE_SCHEMA_UNIQUE = createUniqueKey(databaseTable, databaseTable.IDENTIFIER);
        }
    }
}