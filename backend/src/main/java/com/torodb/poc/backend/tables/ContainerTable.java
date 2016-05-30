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
import com.torodb.poc.backend.tables.records.ContainerRecord;

public abstract class ContainerTable<Record extends ContainerRecord> extends TableImpl<Record> {

    private static final long serialVersionUID = 1664366669485866827L;

    public static final String TABLE_NAME = "container";

    public enum TableFields {
        DATABASE               (   "database"          ),
        COLLECTION             (   "collection"        ),
        PATH                   (   "path"              ),
        TABLE_NAME             (   "table_name"        ),
        PARENT_TABLE_NAME      (   "parent_table_name" ),
        LAST_RID               (   "last_rid"          )
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
     * The column <code>torodb.container.table_name</code>.
     */
    public final TableField<Record, String> PARENT_TABLE 
            = createParentTableNameField();

    /**
     * The column <code>torodb.container.table_name</code>.
     */
    public final TableField<Record, String> TABLE 
            = createTableNameField();

    /**
     * The column <code>torodb.container.last_rid</code>.
     */
    public final TableField<Record, Integer> LAST_RID 
            = createLastRidField();

    protected abstract TableField<Record, String> createDatabaseField();
    protected abstract TableField<Record, String> createCollectionField();
    protected abstract TableField<Record, String> createPathField();
    protected abstract TableField<Record, String> createTableNameField();
    protected abstract TableField<Record, String> createParentTableNameField();
    protected abstract TableField<Record, Integer> createLastRidField();

    private final UniqueKeys<Record> uniqueKeys;
    
    /**
     * Create a <code>torodb.collections</code> table reference
     */
    public ContainerTable() {
        this(TABLE_NAME, null);
    }

    protected ContainerTable(String alias, Table<Record> aliased) {
        this(alias, aliased, null);
    }

    protected ContainerTable(String alias, Table<Record> aliased, Field<?>[] parameters) {
        super(alias, TorodbSchema.TORODB, aliased, parameters, "");
        
        this.uniqueKeys = new UniqueKeys<Record>(this);
    }
    
    public String getSQLCreationStatement(DatabaseInterface databaseInterface) {
        return databaseInterface.createContainerTableStatement(getSchema().getName(), getName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UniqueKey<Record> getPrimaryKey() {
        return uniqueKeys.CONTAINER_PKEY;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<UniqueKey<Record>> getKeys() {
        return Arrays.<UniqueKey<Record>>asList(uniqueKeys.CONTAINER_PKEY, uniqueKeys.CONTAINER_TABLE_NAME_UNIQUE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract ContainerTable<Record> as(String alias);

    /**
     * Rename this table
     */
    public abstract ContainerTable<Record> rename(String name);

    public boolean isSemanticallyEquals(Table<Record> table) {
        if (!table.getName().equals(getName())) {
            return false;
        }
        if (table.getSchema() == null || !getSchema().getName().equals(table.getSchema().getName())) {
            return false;
        }
        if (table.fields().length != 5) {
            return false;
        }
        return true; //TODO: improve the check
    }
    
    public UniqueKeys<Record> getUniqueKeys() {
        return uniqueKeys;
    }
    
    public static class UniqueKeys<KeyRecord extends ContainerRecord> extends AbstractKeys {
        private final UniqueKey<KeyRecord> CONTAINER_PKEY;
        private final UniqueKey<KeyRecord> CONTAINER_TABLE_NAME_UNIQUE;
        
        private UniqueKeys(ContainerTable<KeyRecord> containerTable) {
            CONTAINER_PKEY = createUniqueKey(containerTable, containerTable.DATABASE, containerTable.COLLECTION, containerTable.PATH);
            CONTAINER_TABLE_NAME_UNIQUE = createUniqueKey(containerTable, containerTable.DATABASE, containerTable.COLLECTION, containerTable.TABLE);
        }
    }
}
