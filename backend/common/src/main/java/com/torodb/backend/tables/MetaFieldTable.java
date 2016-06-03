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
import com.torodb.backend.tables.records.MetaFieldRecord;

public abstract class MetaFieldTable<TableRefType, R extends MetaFieldRecord<TableRefType>> extends SemanticTableImpl<R> {

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
    public abstract Class<R> getRecordType();

    /**
     * The column <code>torodb.container.database</code>.
     */
    public final TableField<R, String> DATABASE 
            = createDatabaseField();

    /**
     * The column <code>torodb.container.collection</code>.
     */
    public final TableField<R, String> COLLECTION 
            = createCollectionField();

    /**
     * The column <code>torodb.container.path</code>.
     */
    public final TableField<R, TableRefType> TABLE_REF 
            = createTableRefField();

    /**
     * The column <code>torodb.container.name</code>.
     */
    public final TableField<R, String> NAME 
            = createNameField();

    /**
     * The column <code>torodb.container.column_name</code>.
     */
    public final TableField<R, String> IDENTIFIER 
            = createIdentifierField();

    /**
     * The column <code>torodb.container.column_type</code>.
     */
    public final TableField<R, String> TYPE 
            = createTypeField();

    protected abstract TableField<R, String> createDatabaseField();
    protected abstract TableField<R, String> createCollectionField();
    protected abstract TableField<R, TableRefType> createTableRefField();
    protected abstract TableField<R, String> createNameField();
    protected abstract TableField<R, String> createIdentifierField();
    protected abstract TableField<R, String> createTypeField();

    private final UniqueKeys<TableRefType, R> uniqueKeys;
    
    /**
     * Create a <code>torodb.collections</code> table reference
     */
    public MetaFieldTable() {
        this(TABLE_NAME, null);
    }

    protected MetaFieldTable(String alias, Table<R> aliased) {
        this(alias, aliased, null);
    }

    protected MetaFieldTable(String alias, Table<R> aliased, Field<?>[] parameters) {
        super(alias, TorodbSchema.TORODB, aliased, parameters, "");
        
        this.uniqueKeys = new UniqueKeys<TableRefType, R>(this);
    }
    
    public String getSQLCreationStatement(DatabaseInterface databaseInterface) {
        return databaseInterface.createMetaFieldTableStatement(getSchema().getName(), getName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UniqueKey<R> getPrimaryKey() {
        return uniqueKeys.FIELD_PKEY;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<UniqueKey<R>> getKeys() {
        return Arrays.<UniqueKey<R>>asList(uniqueKeys.FIELD_PKEY, uniqueKeys.FIELD_COLUMN_NAME_UNIQUE_PKEY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract MetaFieldTable<TableRefType, R> as(String alias);

    /**
     * Rename this table
     */
    public abstract MetaFieldTable<TableRefType, R> rename(String name);

    public UniqueKeys<TableRefType, R> getUniqueKeys() {
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
