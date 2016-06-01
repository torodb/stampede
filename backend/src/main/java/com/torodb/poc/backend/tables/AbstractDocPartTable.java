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

package com.torodb.poc.backend.tables;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.Identity;
import org.jooq.Record;
import org.jooq.RecordMapper;
import org.jooq.Schema;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.TableImpl;

import com.google.common.collect.AbstractIterator;
import com.torodb.core.TableRef;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.poc.backend.DatabaseInterface;
import com.torodb.poc.backend.meta.DatabaseSchema;
import com.torodb.poc.backend.tables.records.AbstractDocPartTableRecord;
import com.torodb.poc.backend.tables.records.RootDocPartTableRecord;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 *
 */
public abstract class AbstractDocPartTable<DocPartTableRecord extends AbstractDocPartTableRecord<DocPartTableRecord>> extends TableImpl<DocPartTableRecord> {

    private static final long serialVersionUID = 1L;
    
    public static final String DID_COLUMN_NAME = "did";
    public static final String RID_COLUMN_NAME = "rid";
    public static final String PID_COLUMN_NAME = "pid";
    public static final String SEQ_COLUMN_NAME = "seq";
    
    private static final String[] SPECIAL_COLUMN_NAMES = new String[] {
            DID_COLUMN_NAME,
            RID_COLUMN_NAME,
            PID_COLUMN_NAME,
            SEQ_COLUMN_NAME
    };
    {
        Arrays.sort(SPECIAL_COLUMN_NAMES);
    }

    /**
     * JOOQ cannot fetch tables whose records don't have default constructor, so
     * {@link RootDocPartTableRecord} cannot be fetched. We use this generic table as
     * a table with the same name but a generic record and a
     * {@link RecordMapper} to fetch elements of PathDocTables.
     */
    protected Table<Record> genericTable;

    protected Identity<DocPartTableRecord, Integer> identityRoot;

    protected final DatabaseInterface databaseInterface;
    protected final MetaDocPart metaDocPart;
    
    protected final String database;
    protected final String collection;
    protected final TableRef tableRef;
    
    public AbstractDocPartTable(
            String database,
            String collection,
            TableRef tableRef,
            DatabaseSchema schema,
            String tableName,
            MetaDocPart metaDocPart,
            DatabaseInterface databaseInterface
    ) {
        this(database, collection, tableRef,
                (Schema) schema, tableName, 
                metaDocPart, databaseInterface);
    }
    
    private AbstractDocPartTable(
            String database,
            String collection,
            TableRef tableRef,
            Schema schema,
            String tableName,
            MetaDocPart metaDocPart,
            DatabaseInterface databaseInterface
    ) {
        super(tableName, schema);
        this.database = database;
        this.collection = collection;
        this.tableRef = tableRef;
        metaDocPart.streamFields().forEach(field -> {
            String fieldName = field.getIdentifier();

            DataType<?> dataType = databaseInterface.getDataType(field.getType());
            
            createField(
                    fieldName,
                    dataType,
                    this,
                    "");
        });

        this.databaseInterface = databaseInterface;
        this.metaDocPart = metaDocPart;
    }
    
    public TableRef getTableRef() {
        return tableRef;
    }
    
    @SuppressFBWarnings("SIC_INNER_SHOULD_BE_STATIC_ANON")
    public Iterable<Field<? extends KVValue<? extends Serializable>>> getPathDocFields() {
        final Iterator<Field<? extends KVValue<? extends Serializable>>> iterator
                = new AbstractIterator<Field<? extends KVValue<? extends Serializable>>>() {

                    Field[] fields = fields();
                    int index = 0;

                    @Override
                    protected Field<? extends KVValue<? extends Serializable>> computeNext() {
                        while(isSpecialColumn(fields[index].getName())) {
                            index++;
                        }
                        if (index == fields.length) {
                            endOfData();
                            return null;
                        }
                        return (Field<? extends KVValue<? extends Serializable>>) field(fields[index++]);
                    }
                };
        return new Iterable<Field<? extends KVValue<? extends Serializable>>>() {
            @Override
            public Iterator<Field<? extends KVValue<? extends Serializable>>> iterator() {
                return iterator;
            }
        };
    }

    public Table<Record> getGenericTable() {
        if (genericTable == null) {
            genericTable = DSL.tableByName(getSchema().getName(), getName());
        }
        return genericTable;
    }

    private static boolean isSpecialColumn(String columnName) {
        return Arrays.binarySearch(SPECIAL_COLUMN_NAMES, columnName) >= 0;
    }
    
    @Override
    public DatabaseSchema getSchema() {
        return (DatabaseSchema) super.getSchema();
    }
    
    /**
     * The class holding records for this type
     * <p>
     * @return
     */
    @Override
    public abstract Class<DocPartTableRecord> getRecordType();

    /**
     * {@inheritDoc}
     * <p>
     * @return
     */
    @Override
    public abstract Identity<DocPartTableRecord, Integer> getIdentity();

    /**
     * {@inheritDoc}
     * <p>
     * @param alias
     * @return
     */
    @Override
    public abstract AbstractDocPartTable<DocPartTableRecord> as(String alias);

    /**
     * Rename this table
     * <p>
     * @param name
     * @return
     */
    public abstract AbstractDocPartTable<DocPartTableRecord> rename(String name);

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }
}
