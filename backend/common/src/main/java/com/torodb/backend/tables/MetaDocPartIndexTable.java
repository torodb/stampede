package com.torodb.backend.tables;

import java.util.Arrays;
import java.util.List;

import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.UniqueKey;
import org.jooq.impl.AbstractKeys;

import com.torodb.backend.meta.TorodbSchema;
import com.torodb.backend.tables.records.MetaDocPartIndexRecord;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(
        value = "HE_HASHCODE_NO_EQUALS",
        justification
        = "Equals comparation is done in TableImpl class, which compares schema, name and fields"
)
public abstract class MetaDocPartIndexTable<TableRefType, R extends MetaDocPartIndexRecord<TableRefType>> extends SemanticTable<R> {

    private static final long serialVersionUID = -2876705139919509818L;

    public static final String TABLE_NAME = "doc_part_index";

    public enum TableFields {
        DATABASE        (   "database"          ),
        IDENTIFIER      (   "identifier"        ),
        COLLECTION      (   "collection"        ),
        TABLE_REF       (   "table_ref"         ),
        UNIQUE          (   "unique"            )
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
     * The column <code>torodb.index_field.database</code>.
     */
    public final TableField<R, String> DATABASE 
            = createDatabaseField();

    /**
     * The column <code>torodb.index_field.identifier</code>.
     */
    public final TableField<R, String> IDENTIFIER 
            = createIdentifierField();

    /**
     * The column <code>torodb.index_field.collection</code>.
     */
    public final TableField<R, String> COLLECTION 
            = createCollectionField();

    /**
     * The column <code>torodb.index_field.path</code>.
     */
    public final TableField<R, TableRefType> TABLE_REF 
            = createTableRefField();

    /**
     * The column <code>torodb.index.unique</code>.
     */
    public final TableField<R, Boolean> UNIQUE 
            = createUniqueField();

    protected abstract TableField<R, String> createDatabaseField();
    protected abstract TableField<R, String> createIdentifierField();
    protected abstract TableField<R, String> createCollectionField();
    protected abstract TableField<R, TableRefType> createTableRefField();
    protected abstract TableField<R, Boolean> createUniqueField();

    private final UniqueKeys<TableRefType, R> uniqueKeys;
    
    /**
     * Create a <code>torodb.index_field</code> table reference
     */
    public MetaDocPartIndexTable() {
        this(TABLE_NAME, null);
    }

    protected MetaDocPartIndexTable(String alias, Table<R> aliased) {
        this(alias, aliased, null);
    }

    protected MetaDocPartIndexTable(String alias, Table<R> aliased, Field<?>[] parameters) {
        super(alias, TorodbSchema.TORODB, aliased, parameters, "");
        
        this.uniqueKeys = new UniqueKeys<TableRefType, R>(this);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public UniqueKey<R> getPrimaryKey() {
        return uniqueKeys.FIELD_INDEXED_PKEY;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<UniqueKey<R>> getKeys() {
        return Arrays.<UniqueKey<R>>asList(uniqueKeys.FIELD_INDEXED_PKEY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract MetaDocPartIndexTable<TableRefType, R> as(String alias);

    /**
     * Rename this table
     */
    public abstract MetaDocPartIndexTable<TableRefType, R> rename(String name);

    public UniqueKeys<TableRefType, R> getUniqueKeys() {
        return uniqueKeys;
    }
    
    public static class UniqueKeys<TableRefType, KeyRecord extends MetaDocPartIndexRecord<TableRefType>> extends AbstractKeys {
        private final UniqueKey<KeyRecord> FIELD_INDEXED_PKEY;
        
        private UniqueKeys(MetaDocPartIndexTable<TableRefType, KeyRecord> fieldTable) {
            FIELD_INDEXED_PKEY = createUniqueKey(fieldTable, fieldTable.DATABASE, fieldTable.IDENTIFIER);
        }
    }
}
