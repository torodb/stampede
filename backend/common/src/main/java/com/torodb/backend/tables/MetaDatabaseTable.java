package com.torodb.backend.tables;

import java.util.Arrays;
import java.util.List;

import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.UniqueKey;
import org.jooq.impl.AbstractKeys;
import org.jooq.impl.TableImpl;

import com.torodb.backend.DatabaseInterface;
import com.torodb.backend.meta.TorodbSchema;
import com.torodb.backend.tables.records.MetaDatabaseRecord;

public abstract class MetaDatabaseTable<Record extends MetaDatabaseRecord> extends TableImpl<Record> {

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
    public abstract Class<Record> getRecordType();

    /**
     * The column <code>torodb.database.name</code>.
     */
    public final TableField<Record, String> NAME 
            = createNameField();

    /**
     * The column <code>torodb.database.schema</code>.
     */
    public final TableField<Record, String> IDENTIFIER 
            = createIdentifierField();

    protected abstract TableField<Record, String> createNameField();
    protected abstract TableField<Record, String> createIdentifierField();
    
    private final UniqueKeys<Record> uniqueKeys;
    
    /**
     * Create a <code>torodb.database</code> table reference
     */
    public MetaDatabaseTable() {
        this(TABLE_NAME, null);
    }

    protected MetaDatabaseTable(String alias, Table<Record> aliased) {
        this(alias, aliased, null);
    }

    protected MetaDatabaseTable(String alias, Table<Record> aliased, Field<?>[] parameters) {
        super(alias, TorodbSchema.TORODB, aliased, parameters, "");
        
        this.uniqueKeys = new UniqueKeys<Record>(this);
    }
    
    public String getSQLCreationStatement(DatabaseInterface databaseInterface) {
        return databaseInterface.createDatabaseTableStatement(getSchema().getName(), getName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UniqueKey<Record> getPrimaryKey() {
        return uniqueKeys.DATABASE_PKEY;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<UniqueKey<Record>> getKeys() {
        return Arrays.<UniqueKey<Record>>asList(uniqueKeys.DATABASE_PKEY, 
                uniqueKeys.DATABASE_SCHEMA_UNIQUE
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract MetaDatabaseTable<Record> as(String alias);

    /**
     * Rename this table
     */
    public abstract MetaDatabaseTable<Record> rename(String name);

    public boolean isSemanticallyEquals(Table<Record> table) {
        if (!table.getName().equals(getName())) {
            return false;
        }
        if (table.getSchema() == null || !getSchema().getName().equals(table.getSchema().getName())) {
            return false;
        }
        if (table.fields().length != 2) {
            return false;
        }
        return true; //TODO: improve the check
    }
    
    public UniqueKeys<Record> getUniqueKeys() {
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
