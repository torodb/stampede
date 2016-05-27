package com.torodb.poc.backend.tables;

import java.util.Arrays;
import java.util.List;

import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.UniqueKey;
import org.jooq.impl.AbstractKeys;
import org.jooq.impl.TableImpl;

import com.torodb.poc.backend.DatabaseInterface;
import com.torodb.poc.backend.meta.TorodbSchema;
import com.torodb.poc.backend.tables.records.FieldRecord;

public abstract class FieldTable<Record extends FieldRecord> extends TableImpl<Record> {

    private static final long serialVersionUID = -3500177946436569355L;

    public static final String TABLE_NAME = "field";

    public enum TableFields {
        DATABASE        (   "database"          ),
        COLLECTION      (   "collection"        ),
        PATH            (   "path"              ),
        NAME            (   "name"              ),
        COLUMN_NAME     (   "column_name"       ),
        COLUMN_TYPE     (   "column_type"       )
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
     * The column <code>torodb.container.database</code>.
     */
    public final TableField<Record, String> DATABASE 
            = createDatabaseField();

    /**
     * The column <code>torodb.container.collection</code>.
     */
    public final TableField<Record, String> COLLECTION 
            = createCollectionField();

    /**
     * The column <code>torodb.container.path</code>.
     */
    public final TableField<Record, String> PATH 
            = createPathField();

    /**
     * The column <code>torodb.container.name</code>.
     */
    public final TableField<Record, String> NAME 
            = createNameField();

    /**
     * The column <code>torodb.container.column_name</code>.
     */
    public final TableField<Record, String> COLUMN_NAME 
            = createColumnNameField();

    /**
     * The column <code>torodb.container.column_type</code>.
     */
    public final TableField<Record, String> COLUMN_TYPE 
            = createColumnTypeField();

    protected abstract TableField<Record, String> createDatabaseField();
    protected abstract TableField<Record, String> createCollectionField();
    protected abstract TableField<Record, String> createPathField();
    protected abstract TableField<Record, String> createNameField();
    protected abstract TableField<Record, String> createColumnNameField();
    protected abstract TableField<Record, String> createColumnTypeField();

    private final UniqueKeys<Record> uniqueKeys;
    
    /**
     * Create a <code>torodb.collections</code> table reference
     */
    public FieldTable() {
        this(TABLE_NAME, null);
    }

    protected FieldTable(String alias, Table<Record> aliased) {
        this(alias, aliased, null);
    }

    protected FieldTable(String alias, Table<Record> aliased, Field<?>[] parameters) {
        super(alias, TorodbSchema.TORODB, aliased, parameters, "");
        
        this.uniqueKeys = new UniqueKeys<Record>(this);
    }
    
    public String getSQLCreationStatement(DatabaseInterface databaseInterface) {
        return databaseInterface.createFieldTableStatement(getSchema().getName(), getName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UniqueKey<Record> getPrimaryKey() {
        return uniqueKeys.FIELD_PKEY;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<UniqueKey<Record>> getKeys() {
        return Arrays.<UniqueKey<Record>>asList(uniqueKeys.FIELD_PKEY, uniqueKeys.FIELD_COLUMN_NAME_UNIQUE_PKEY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract FieldTable<Record> as(String alias);

    /**
     * Rename this table
     */
    public abstract FieldTable<Record> rename(String name);

    public boolean isSemanticallyEquals(Table<Record> table) {
        if (!table.getName().equals(getName())) {
            return false;
        }
        if (table.getSchema() == null || !getSchema().getName().equals(table.getSchema().getName())) {
            return false;
        }
        if (table.fields().length != 6) {
            return false;
        }
        return true; //TODO: improve the check
    }
    
    public UniqueKeys<Record> getUniqueKeys() {
        return uniqueKeys;
    }
    
    public static class UniqueKeys<KeyRecord extends FieldRecord> extends AbstractKeys {
        private final UniqueKey<KeyRecord> FIELD_PKEY;
        private final UniqueKey<KeyRecord> FIELD_COLUMN_NAME_UNIQUE_PKEY;
        
        private UniqueKeys(FieldTable<KeyRecord> fieldTable) {
            FIELD_PKEY = createUniqueKey(fieldTable, fieldTable.DATABASE, fieldTable.COLLECTION, fieldTable.PATH, fieldTable.NAME);
            FIELD_COLUMN_NAME_UNIQUE_PKEY = createUniqueKey(fieldTable, fieldTable.DATABASE, fieldTable.COLLECTION, fieldTable.PATH, fieldTable.COLUMN_NAME);
        }
    }
}
