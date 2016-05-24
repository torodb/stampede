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
import org.jooq.TableField;
import org.jooq.impl.AbstractKeys;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.impl.TableImpl;

import com.google.common.collect.AbstractIterator;
import com.torodb.poc.backend.DatabaseInterface;
import com.torodb.poc.backend.meta.DatabaseSchema;
import com.torodb.poc.backend.tables.records.FieldRecord;
import com.torodb.poc.backend.tables.records.PathDocTableRecord;
import com.torodb.torod.core.subdocument.values.ScalarValue;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 *
 */
public class PathDocTable extends TableImpl<PathDocTableRecord> {
    
    private static final long serialVersionUID = 1197457693;
    
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
     * {@link PathDocTableRecord} cannot be fetched. We use this generic table as
     * a table with the same name but a generic record and a
     * {@link RecordMapper} to fetch elements of PathDocTables.
     */
    private Table<Record> genericTable;

    private Identity<PathDocTableRecord, Integer> identityRoot;

    private final TableField<PathDocTableRecord, Integer> didField
            = createField(DID_COLUMN_NAME, SQLDataType.INTEGER.nullable(false), this, "");
    private final TableField<PathDocTableRecord, Integer> ridField
            = createField(DID_COLUMN_NAME, SQLDataType.INTEGER.nullable(false), this, "");
    private final TableField<PathDocTableRecord, Integer> pidField
            = createField(DID_COLUMN_NAME, SQLDataType.INTEGER.nullable(false), this, "");
    private final TableField<PathDocTableRecord, Integer> seqField
            = createField(SEQ_COLUMN_NAME, SQLDataType.INTEGER.nullable(true), this, "");

    private final DatabaseInterface databaseInterface;
    
    private final String database;
    private final String collection;
    private final String path;
    
    public PathDocTable(
            String database,
            String collection,
            String path,
            DatabaseSchema schema,
            String tableName,
            DatabaseInterface databaseInterface
    ) {
        this(database, collection, path, (Schema) schema, tableName, databaseInterface);
    }
    
    private PathDocTable(
            String database,
            String collection,
            String path,
            Schema schema,
            String tableName,
            DatabaseInterface databaseInterface
    ) {
        super(tableName, schema);
        this.database = database;
        this.collection = collection;
        this.path = path;
        for (FieldRecord fieldRecord : databaseInterface.getFields(database, collection, path)) {
            String fieldName = fieldRecord.getColumnName();

            DataType<?> dataType = databaseInterface.getDataType(fieldRecord.getColumnType());
            
            createField(
                    fieldName,
                    dataType,
                    this,
                    "");
        }

        this.databaseInterface = databaseInterface;
    }
    
    public String getPath() {
        return path;
    }

    @SuppressFBWarnings("SIC_INNER_SHOULD_BE_STATIC_ANON")
    public Iterable<Field<? extends ScalarValue<? extends Serializable>>> getPathDocFields() {
        final Iterator<Field<? extends ScalarValue<? extends Serializable>>> iterator
                = new AbstractIterator<Field<? extends ScalarValue<? extends Serializable>>>() {

                    Field[] fields = fields();
                    int index = 0;

                    @Override
                    protected Field<? extends ScalarValue<? extends Serializable>> computeNext() {
                        while(isSpecialColumn(fields[index].getName())) {
                            index++;
                        }
                        if (index == fields.length) {
                            endOfData();
                            return null;
                        }
                        return (Field<? extends ScalarValue<? extends Serializable>>) field(fields[index++]);
                    }
                };
        return new Iterable<Field<? extends ScalarValue<? extends Serializable>>>() {
            @Override
            public Iterator<Field<? extends ScalarValue<? extends Serializable>>> iterator() {
                return iterator;
            }
        };
    }

    public TableField<PathDocTableRecord, Integer> getDidColumn() {
        return didField;
    }

    public TableField<PathDocTableRecord, Integer> getRidColumn() {
        return ridField;
    }

    public TableField<PathDocTableRecord, Integer> getPidColumn() {
        return pidField;
    }

    public TableField<PathDocTableRecord, Integer> getSeqColumn() {
        return seqField;
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
    public PathDocTable as(String alias) {
        return new PathDocTable(database, collection, path, getSchema(), alias, databaseInterface);
    }

    /**
     * Rename this table
     * <p>
     * @param name
     * @return
     */
    public PathDocTable rename(String name) {
        return new PathDocTable(database, collection, path, getSchema(), name, databaseInterface);
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

        public static Identity<PathDocTableRecord, Integer> createIdentity(PathDocTable table) {
            return createIdentity(table, table.didField);
        }
    }
}
