/*
 * ToroDB - ToroDB-poc: Backend common
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.torodb.backend.tables;

import java.util.Arrays;
import java.util.List;

import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.UniqueKey;
import org.jooq.impl.AbstractKeys;

import com.torodb.backend.meta.TorodbSchema;
import com.torodb.backend.tables.records.MetaIndexFieldRecord;
import com.torodb.core.transaction.metainf.FieldIndexOrdering;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(
        value = "HE_HASHCODE_NO_EQUALS",
        justification
        = "Equals comparation is done in TableImpl class, which compares schema, name and fields"
)
public abstract class MetaIndexFieldTable<TableRefType, R extends MetaIndexFieldRecord<TableRefType>> extends SemanticTable<R> {

    private static final long serialVersionUID = -7521772531370036757L;

    public static final String TABLE_NAME = "index_field";

    public enum TableFields {
        DATABASE        (   "database"          ),
        COLLECTION      (   "collection"        ),
        INDEX           (   "index"             ),
        POSITION        (   "position"          ),
        TABLE_REF       (   "table_ref"         ),
        NAME            (   "name"              ),
        ORDERING        (   "ordering"          )
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
     * The column <code>torodb.index_field.collection</code>.
     */
    public final TableField<R, String> COLLECTION 
            = createCollectionField();

    /**
     * The column <code>torodb.index_field.index</code>.
     */
    public final TableField<R, String> INDEX 
            = createIndexField();

    /**
     * The column <code>torodb.index_field.position</code>.
     */
    public final TableField<R, Integer> POSITION 
            = createPositionField();

    /**
     * The column <code>torodb.index_field.path</code>.
     */
    public final TableField<R, TableRefType> TABLE_REF 
            = createTableRefField();

    /**
     * The column <code>torodb.index_field.name</code>.
     */
    public final TableField<R, String> NAME 
            = createNameField();

    /**
     * The column <code>torodb.index_field.ordering</code>.
     */
    public final TableField<R, FieldIndexOrdering> ORDERING 
            = createOrderingField();

    protected abstract TableField<R, String> createDatabaseField();
    protected abstract TableField<R, String> createCollectionField();
    protected abstract TableField<R, String> createIndexField();
    protected abstract TableField<R, Integer> createPositionField();
    protected abstract TableField<R, TableRefType> createTableRefField();
    protected abstract TableField<R, String> createNameField();
    protected abstract TableField<R, FieldIndexOrdering> createOrderingField();

    private final UniqueKeys<TableRefType, R> uniqueKeys;
    
    /**
     * Create a <code>torodb.index_field</code> table reference
     */
    public MetaIndexFieldTable() {
        this(TABLE_NAME, null);
    }

    protected MetaIndexFieldTable(String alias, Table<R> aliased) {
        this(alias, aliased, null);
    }

    protected MetaIndexFieldTable(String alias, Table<R> aliased, Field<?>[] parameters) {
        super(alias, TorodbSchema.TORODB, aliased, parameters, "");
        
        this.uniqueKeys = new UniqueKeys<TableRefType, R>(this);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public UniqueKey<R> getPrimaryKey() {
        return uniqueKeys.INDEX_FIELD_PKEY;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<UniqueKey<R>> getKeys() {
        return Arrays.<UniqueKey<R>>asList(uniqueKeys.INDEX_FIELD_PKEY,
                uniqueKeys.TABLE_REF_NAME_UNIQUE
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract MetaIndexFieldTable<TableRefType, R> as(String alias);

    /**
     * Rename this table
     */
    public abstract MetaIndexFieldTable<TableRefType, R> rename(String name);

    public UniqueKeys<TableRefType, R> getUniqueKeys() {
        return uniqueKeys;
    }
    
    public static class UniqueKeys<TableRefType, KeyRecord extends MetaIndexFieldRecord<TableRefType>> extends AbstractKeys {
        private final UniqueKey<KeyRecord> INDEX_FIELD_PKEY;
        private final UniqueKey<KeyRecord> TABLE_REF_NAME_UNIQUE;
        
        private UniqueKeys(MetaIndexFieldTable<TableRefType, KeyRecord> fieldTable) {
            INDEX_FIELD_PKEY = createUniqueKey(fieldTable, fieldTable.DATABASE, fieldTable.COLLECTION, fieldTable.INDEX, fieldTable.POSITION);
            TABLE_REF_NAME_UNIQUE = createUniqueKey(fieldTable, fieldTable.DATABASE, fieldTable.COLLECTION, fieldTable.INDEX, fieldTable.TABLE_REF, fieldTable.NAME);
        }
    }
}
