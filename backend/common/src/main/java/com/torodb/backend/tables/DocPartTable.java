/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.torodb.backend.tables;

import java.util.List;

import org.jooq.ForeignKey;
import org.jooq.Identity;
import org.jooq.TableField;
import org.jooq.UniqueKey;
import org.jooq.impl.AbstractKeys;
import org.jooq.impl.SQLDataType;

import com.google.common.collect.ImmutableList;
import com.torodb.backend.DatabaseInterface;
import com.torodb.backend.meta.DatabaseSchema;
import com.torodb.backend.tables.records.DocPartTableRecord;
import com.torodb.core.TableRef;
import com.torodb.core.transaction.metainf.MetaDocPart;

/**
 *
 */
public class DocPartTable extends AbstractDocPartTable<DocPartTableRecord> {
    
    private static final long serialVersionUID = 1197457693;
    
    private final TableField<DocPartTableRecord, Integer> didField
            = createField(DID_COLUMN_NAME, SQLDataType.INTEGER.nullable(false), this, "");
    private final TableField<DocPartTableRecord, Integer> ridField
            = createField(RID_COLUMN_NAME, SQLDataType.INTEGER.nullable(false), this, "");
    private final TableField<DocPartTableRecord, Integer> pidField
            = createField(PID_COLUMN_NAME, SQLDataType.INTEGER.nullable(false), this, "");
    private final TableField<DocPartTableRecord, Integer> seqField
            = createField(SEQ_COLUMN_NAME, SQLDataType.INTEGER.nullable(true), this, "");
    
    private final String parentTableName;

    public DocPartTable(String database, String collection, TableRef tableRef, DatabaseSchema schema, String tableName, String parentTableName,
            MetaDocPart metaDocPart, DatabaseInterface databaseInterface) {
        super(database, collection, tableRef, schema, tableName, metaDocPart, databaseInterface);
        
        this.parentTableName = parentTableName;
    }

    public TableField<DocPartTableRecord, Integer> getDidColumn() {
        return didField;
    }

    public TableField<DocPartTableRecord, Integer> getRidColumn() {
        return ridField;
    }

    public TableField<DocPartTableRecord, Integer> getPidColumn() {
        return pidField;
    }

    public TableField<DocPartTableRecord, Integer> getSeqColumn() {
        return seqField;
    }

    @Override
    public Class<DocPartTableRecord> getRecordType() {
        return DocPartTableRecord.class;
    }

    /**
     * {@inheritDoc}
     * <p>
     * @return
     */
    @Override
    public Identity<DocPartTableRecord, Integer> getIdentity() {
        if (identityRoot == null) {
            synchronized (this) {
                identityRoot = IdentityFactory.createIdentity(this);
            }
        }
        return identityRoot;
    }

    @Override
    public List<ForeignKey<DocPartTableRecord, ?>> getReferences() {
        ImmutableList.Builder<ForeignKey<DocPartTableRecord,?>> referencesBuilder =
                ImmutableList.builder();
        if(tableRef.getParent().isPresent()) {
            referencesBuilder.add(ForeignKeyFactory.createForeignKeyToParent(this));
        } else {
            referencesBuilder.add(ForeignKeyFactory.createForeignKeyToRoot(this));
        }
        return referencesBuilder.build();
    }

    @Override
    public List<UniqueKey<DocPartTableRecord>> getKeys() {
        ImmutableList.Builder<UniqueKey<DocPartTableRecord>> keysBuilder =
                ImmutableList.builder();
        keysBuilder.add(UniqueKeyFactory.createDidUniqueKey(this));
        return keysBuilder.build();
    }

    /**
     * {@inheritDoc}
     * <p>
     * @param alias
     * @return
     */
    @Override
    public DocPartTable as(String alias) {
        return new DocPartTable(database, collection, tableRef, getSchema(), 
                alias, parentTableName, metaDocPart, databaseInterface);
    }

    /**
     * Rename this table
     * <p>
     * @param name
     * @return
     */
    public DocPartTable rename(String name) {
        return new DocPartTable(database, collection, tableRef, getSchema(), 
                name, parentTableName, metaDocPart, databaseInterface);
    }

    private static class IdentityFactory extends AbstractKeys {
        public static Identity<DocPartTableRecord, Integer> createIdentity(DocPartTable table) {
            return createIdentity(table, table.ridField);
        }
    }

    private static class ForeignKeyFactory extends AbstractKeys {
        public static ForeignKey<DocPartTableRecord, DocPartTableRecord> createForeignKeyToParent(DocPartTable table) {
            return createForeignKey(UniqueKeyFactory.createPidUniqueKey(table), table, table.parentTableName, table.ridField);
        }
        public static ForeignKey<DocPartTableRecord, DocPartTableRecord> createForeignKeyToRoot(DocPartTable table) {
            return createForeignKey(UniqueKeyFactory.createDidUniqueKey(table), table, table.parentTableName, table.didField);
        }
    }

    private static class UniqueKeyFactory extends AbstractKeys {
        public static UniqueKey<DocPartTableRecord> createDidUniqueKey(DocPartTable table) {
            return createUniqueKey(table, table.didField);
        }
        public static UniqueKey<DocPartTableRecord> createPidUniqueKey(DocPartTable table) {
            return createUniqueKey(table, table.pidField);
        }
    }
}
