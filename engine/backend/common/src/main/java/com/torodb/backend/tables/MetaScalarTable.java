/*
 * ToroDB - ToroDB: Backend common
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
import com.torodb.backend.tables.records.MetaScalarRecord;
import com.torodb.core.transaction.metainf.FieldType;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(
        value = "HE_HASHCODE_NO_EQUALS",
        justification
        = "Equals comparation is done in TableImpl class, which compares schema, name and fields"
)
public abstract class MetaScalarTable<TableRefType, R extends MetaScalarRecord<TableRefType>> extends SemanticTable<R> {

    private static final long serialVersionUID = -1500177946436569355L;

    public static final String TABLE_NAME = "scalar";

    public enum TableFields {
        DATABASE        (   "database"          ),
        COLLECTION      (   "collection"        ),
        TABLE_REF       (   "table_ref"         ),
        TYPE            (   "type"              ),
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
     * The column <code>torodb.scalar.database</code>.
     */
    public final TableField<R, String> DATABASE 
            = createDatabaseField();

    /**
     * The column <code>torodb.scalar.collection</code>.
     */
    public final TableField<R, String> COLLECTION 
            = createCollectionField();

    /**
     * The column <code>torodb.scalar.path</code>.
     */
    public final TableField<R, TableRefType> TABLE_REF 
            = createTableRefField();

    /**
     * The column <code>torodb.scalar.type</code>.
     */
    public final TableField<R, FieldType> TYPE 
            = createTypeField();

    /**
     * The column <code>torodb.scalar.identifier</code>.
     */
    public final TableField<R, String> IDENTIFIER 
            = createIdentifierField();

    protected abstract TableField<R, String> createDatabaseField();
    protected abstract TableField<R, String> createCollectionField();
    protected abstract TableField<R, TableRefType> createTableRefField();
    protected abstract TableField<R, FieldType> createTypeField();
    protected abstract TableField<R, String> createIdentifierField();

    private final UniqueKeys<TableRefType, R> uniqueKeys;
    
    /**
     * Create a <code>torodb.scalar</code> table reference
     */
    public MetaScalarTable() {
        this(TABLE_NAME, null);
    }

    protected MetaScalarTable(String alias, Table<R> aliased) {
        this(alias, aliased, null);
    }

    protected MetaScalarTable(String alias, Table<R> aliased, Field<?>[] parameters) {
        super(alias, TorodbSchema.TORODB, aliased, parameters, "");
        
        this.uniqueKeys = new UniqueKeys<TableRefType, R>(this);
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
        return Arrays.<UniqueKey<R>>asList(uniqueKeys.FIELD_PKEY, 
                uniqueKeys.FIELD_COLUMN_NAME_UNIQUE_PKEY
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract MetaScalarTable<TableRefType, R> as(String alias);

    /**
     * Rename this table
     */
    public abstract MetaScalarTable<TableRefType, R> rename(String name);

    public UniqueKeys<TableRefType, R> getUniqueKeys() {
        return uniqueKeys;
    }
    
    public static class UniqueKeys<TableRefType, KeyRecord extends MetaScalarRecord<TableRefType>> extends AbstractKeys {
        private final UniqueKey<KeyRecord> FIELD_PKEY;
        private final UniqueKey<KeyRecord> FIELD_COLUMN_NAME_UNIQUE_PKEY;
        
        private UniqueKeys(MetaScalarTable<TableRefType, KeyRecord> fieldTable) {
            FIELD_PKEY = createUniqueKey(fieldTable, fieldTable.DATABASE, fieldTable.COLLECTION, fieldTable.TABLE_REF, fieldTable.TYPE);
            FIELD_COLUMN_NAME_UNIQUE_PKEY = createUniqueKey(fieldTable, fieldTable.DATABASE, fieldTable.TABLE_REF, fieldTable.IDENTIFIER);
        }
    }
}