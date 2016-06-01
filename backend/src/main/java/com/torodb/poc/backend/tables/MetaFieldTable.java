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
import com.torodb.poc.backend.tables.records.MetaFieldRecord;

public abstract class MetaFieldTable<TableRefType, Record extends MetaFieldRecord<TableRefType>> extends TableImpl<Record> {

    private static final long serialVersionUID = -3500177946436569355L;

    public static final String TABLE_NAME = "field";

    public enum TableFields {
        DATABASE        (   "database"          ),
        COLLECTION      (   "collection"        ),
        TABLE_REF       (   "tableRef"          ),
        NAME            (   "name"              ),
        IDENTIFIER      (   "identifier"        ),
        TYPE            (   "type"              )
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
    public final TableField<Record, TableRefType> TABLE_REF 
            = createTableRefField();

    /**
     * The column <code>torodb.container.name</code>.
     */
    public final TableField<Record, String> NAME 
            = createNameField();

    /**
     * The column <code>torodb.container.column_name</code>.
     */
    public final TableField<Record, String> IDENTIFIER 
            = createIdentifierField();

    /**
     * The column <code>torodb.container.column_type</code>.
     */
    public final TableField<Record, String> TYPE 
            = createTypeField();

    protected abstract TableField<Record, String> createDatabaseField();
    protected abstract TableField<Record, String> createCollectionField();
    protected abstract TableField<Record, TableRefType> createTableRefField();
    protected abstract TableField<Record, String> createNameField();
    protected abstract TableField<Record, String> createIdentifierField();
    protected abstract TableField<Record, String> createTypeField();

    private final UniqueKeys<TableRefType, Record> uniqueKeys;
    
    /**
     * Create a <code>torodb.collections</code> table reference
     */
    public MetaFieldTable() {
        this(TABLE_NAME, null);
    }

    protected MetaFieldTable(String alias, Table<Record> aliased) {
        this(alias, aliased, null);
    }

    protected MetaFieldTable(String alias, Table<Record> aliased, Field<?>[] parameters) {
        super(alias, TorodbSchema.TORODB, aliased, parameters, "");
        
        this.uniqueKeys = new UniqueKeys<TableRefType, Record>(this);
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
    public abstract MetaFieldTable<TableRefType, Record> as(String alias);

    /**
     * Rename this table
     */
    public abstract MetaFieldTable<TableRefType, Record> rename(String name);

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
    
    public UniqueKeys<TableRefType, Record> getUniqueKeys() {
        return uniqueKeys;
    }
    
    public static class UniqueKeys<TableRefType, KeyRecord extends MetaFieldRecord<TableRefType>> extends AbstractKeys {
        private final UniqueKey<KeyRecord> FIELD_PKEY;
        private final UniqueKey<KeyRecord> FIELD_COLUMN_NAME_UNIQUE_PKEY;
        
        private UniqueKeys(MetaFieldTable<TableRefType, KeyRecord> fieldTable) {
            FIELD_PKEY = createUniqueKey(fieldTable, fieldTable.DATABASE, fieldTable.COLLECTION, fieldTable.TABLE_REF, fieldTable.NAME);
            FIELD_COLUMN_NAME_UNIQUE_PKEY = createUniqueKey(fieldTable, fieldTable.DATABASE, fieldTable.COLLECTION, fieldTable.TABLE_REF, fieldTable.IDENTIFIER);
        }
    }
}
