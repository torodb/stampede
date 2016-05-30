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
import java.util.Map;

import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.Identity;
import org.jooq.Record;
import org.jooq.RecordMapper;
import org.jooq.Schema;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.AbstractKeys;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.impl.TableImpl;

import com.google.common.collect.AbstractIterator;
import com.torodb.kvdocument.types.KVType;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.poc.backend.DatabaseInterface;
import com.torodb.poc.backend.meta.DatabaseSchema;
import com.torodb.poc.backend.mocks.PathDocStructure;
import com.torodb.poc.backend.tables.records.PathDocTableRecord;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 *
 */
public class RootDocTable extends TableImpl<PathDocTableRecord> {
    
    private static final long serialVersionUID = 2532216259252881711L;

    public static final String DID_COLUMN_NAME = "did";
    
    private static final String[] SPECIAL_COLUMN_NAMES = new String[] {
            DID_COLUMN_NAME
    };
    {
        Arrays.sort(SPECIAL_COLUMN_NAMES);
    }

    /**
     * JOOQ cannot fetch tables whose records don't have default constructor, so
     * {@link PathDocTableRecord} cannot be fetched. We use this generic table as
     * a table with the same name but a generic record and a
     * {@link RecordMapper} to fetch elements of PathDocTables.
     */
    private Table<Record> genericTable;

    private Identity<PathDocTableRecord, Integer> identityRoot;

    private final TableField<PathDocTableRecord, Integer> didField
            = createField(DID_COLUMN_NAME, SQLDataType.INTEGER.nullable(false), this, "");

    private final DatabaseInterface databaseInterface;
    private final PathDocStructure pathDocStructure;
    
    private final String database;
    private final String collection;
    
    public RootDocTable(
            String database,
            String collection,
            DatabaseSchema schema,
            String tableName,
            PathDocStructure pathDocStructure,
            DatabaseInterface databaseInterface
    ) {
        this(database, collection, 
                (Schema) schema, tableName, 
                pathDocStructure, databaseInterface);
    }
    
    private RootDocTable(
            String database,
            String collection,
            Schema schema,
            String tableName,
            PathDocStructure pathDocStructure,
            DatabaseInterface databaseInterface
    ) {
        super(tableName, schema);
        this.database = database;
        this.collection = collection;
        for (Map.Entry<String, KVType> field : pathDocStructure.getFields().entrySet()) {
            String fieldName = field.getKey();

            DataType<?> dataType = databaseInterface.getDataType(field.getValue());
            
            createField(
                    fieldName,
                    dataType,
                    this,
                    "");
        }

        this.databaseInterface = databaseInterface;
        this.pathDocStructure = pathDocStructure;
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

    public TableField<PathDocTableRecord, Integer> getDidColumn() {
        return didField;
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

    /**
     * The class holding records for this type
     * <p>
     * @return
     */
    @Override
    public Class<PathDocTableRecord> getRecordType() {
        return PathDocTableRecord.class;
    }

    /**
     * {@inheritDoc}
     * <p>
     * @return
     */
    @Override
    public Identity<PathDocTableRecord, Integer> getIdentity() {
        if (identityRoot == null) {
            synchronized (this) {
                identityRoot = IdentityFactory.createIdentity(this);
            }
        }
        return identityRoot;
    }

    /**
     * {@inheritDoc}
     * <p>
     * @param alias
     * @return
     */
    @Override
    public RootDocTable as(String alias) {
        return new RootDocTable(database, collection, getSchema(), 
                alias, pathDocStructure, databaseInterface);
    }

    /**
     * Rename this table
     * <p>
     * @param name
     * @return
     */
    public RootDocTable rename(String name) {
        return new RootDocTable(database, collection, getSchema(), 
                name, pathDocStructure, databaseInterface);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    private static class IdentityFactory extends AbstractKeys {
        public static Identity<PathDocTableRecord, Integer> createIdentity(RootDocTable table) {
            return createIdentity(table, table.didField);
        }
    }
}
