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

package com.torodb.torod.db.postgresql.meta.tables;

import com.google.common.collect.AbstractIterator;
import com.torodb.torod.core.exceptions.ToroImplementationException;
import com.torodb.torod.core.subdocument.*;
import com.torodb.torod.db.postgresql.IdsFilter;
import com.torodb.torod.db.postgresql.converters.jooq.SubdocValueConverter;
import com.torodb.torod.db.postgresql.converters.jooq.ValueToJooqConverterProvider;
import com.torodb.torod.db.postgresql.meta.CollectionSchema;
import com.torodb.torod.db.postgresql.meta.tables.records.SubDocTableRecord;
import com.torodb.torod.core.subdocument.values.Value;
import com.torodb.torod.db.postgresql.converters.BasicTypeToSqlType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.Serializable;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import org.jooq.*;
import org.jooq.impl.AbstractKeys;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.impl.TableImpl;

/**
 *
 */
public class SubDocTable extends TableImpl<SubDocTableRecord> {

    private static final long serialVersionUID = 1197457693;
    private static final Pattern TABLE_ID_PATTERN
            = Pattern.compile("t_([0-9]+)$");
    private static final Pattern LIKE_DID_PATTERN = Pattern.compile("_*did");
    private static final Pattern LIKE_INDEX_PATTERN = Pattern.compile("_*index");
    public static final String DID_COLUMN_NAME = "did";
    public static final String INDEX_COLUMN_NAME = "index";

    private final SubDocType erasuredType;
    /**
     * JOOQ cannot fetch tables whose records don't have default constructor, so
     * {@link SubDocTableRecord} cannot be fetched. We use this generic table as
     * a table with the same name but a generic record and a
     * {@link RecordMapper} to fetch elements of SubDocTables.
     */
    private Table<Record> genericTable;

    private Identity<SubDocTableRecord, Integer> identityRoot;

    private final TableField<SubDocTableRecord, Integer> didField
            = createField(DID_COLUMN_NAME, SQLDataType.INTEGER.nullable(false), this, "");
    private final TableField<SubDocTableRecord, Integer> indexField
            = createField(INDEX_COLUMN_NAME, SQLDataType.INTEGER.nullable(true), this, "");

    public SubDocTable(
            String tableName,
            CollectionSchema schema,
            DatabaseMetaData metadata
    ) {
        this(
                tableName,
                schema,
                null,
                extractSimpleType(
                        schema.getName(),
                        tableName,
                        metadata
                )
        );
    }

    public SubDocTable(CollectionSchema schema, SubDocType type, int typeId) {
        this(getSubDocTableName(typeId), schema, null, type);
    }

    private SubDocTable(String alias, Schema schema, Table<SubDocTableRecord> aliased, @Nonnull SubDocType type) {
        this(alias, schema, aliased, null, type);
    }

    private SubDocTable(String alias, Schema schema, Table<SubDocTableRecord> aliased, Field<?>[] parameters, @Nonnull SubDocType type) {
        super(alias, schema, aliased, parameters, "");

        this.erasuredType = type;

        for (SubDocAttribute attibute : type.getAttributes()) {
            String fieldName = toColumnName(attibute.getKey());

            SubdocValueConverter converter
                    = ValueToJooqConverterProvider.getConverter(attibute.getType());
            createField(
                    fieldName,
                    converter.getDataType(),
                    this,
                    "",
                    converter);
        }

    }

    @SuppressFBWarnings("SIC_INNER_SHOULD_BE_STATIC_ANON")
    public Iterable<Field<? extends Value<? extends Serializable>>> getSubDocFields() {
        final Iterator<Field<? extends Value<? extends Serializable>>> iterator
                = new AbstractIterator<Field<? extends Value<? extends Serializable>>>() {

                    Iterator<String> attIt
                    = erasuredType.getAttributeKeys().iterator();

                    @Override
                    protected Field<? extends Value<? extends Serializable>> computeNext() {
                        if (!attIt.hasNext()) {
                            endOfData();
                            return null;
                        }
                        return (Field<? extends Value<? extends Serializable>>) field(attIt.next());
                    }
                };
        return new Iterable<Field<? extends Value<? extends Serializable>>>() {

            @Override
            public Iterator<Field<? extends Value<? extends Serializable>>> iterator() {
                return iterator;
            }
        };
    }

    public static boolean isSubDocTable(String name) {
        return getSubDocTypeId(name) != null;
    }

    private static Integer getSubDocTypeId(String name) {
        Matcher matcher = TABLE_ID_PATTERN.matcher(name);
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        }
        return null;
    }

    private static String getSubDocTableName(int typeId) {
        return "t_" + typeId;
    }

    private static SubDocType extractSimpleType(
            String schemaName,
            String tableName,
            DatabaseMetaData metadata
    ) {
        try {
            SubDocType.Builder builder = new SubDocType.Builder();

            ResultSet columns
                    = metadata.getColumns(null, schemaName, tableName, null);

            while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                if (SubDocTable.isSpecialColumn(columnName)) {
                    continue;
                }
                int intColumnType = columns.getInt("DATA_TYPE");
                String stringColumnType = columns.getString("TYPE_NAME");

                BasicType basicType = BasicTypeToSqlType.getInstance().toBasicType(
                        columnName,
                        intColumnType,
                        stringColumnType
                );
                String attName = toAttributeName(columnName);

                builder.add(new SubDocAttribute(attName, basicType));
            }

            return builder.build();
        }
        catch (SQLException ex) {
            throw new ToroImplementationException(
                    "Torod cannot start",
                    ex
            );
        }
    }

    public TableField<SubDocTableRecord, Integer> getDidColumn() {
        return didField;
    }

    public TableField<SubDocTableRecord, Integer> getIndexColumn() {
        return indexField;
    }

    public Table<Record> getGenericTable() {
        if (genericTable == null) {
            genericTable = DSL.tableByName(getSchema().getName(), getName());
        }
        return genericTable;
    }

    public int getTypeId() {
        Integer id = SubDocTable.getSubDocTypeId(getName());
        assert id != null;
        return id;
    }

    public SubDocType getSubDocType() {
        return erasuredType;
    }

    private static boolean isSpecialColumn(String columnName) {
        return columnName.equals(DID_COLUMN_NAME)
                || columnName.equals(INDEX_COLUMN_NAME);
    }

    public static String toColumnName(String attName) {
        Matcher matcher = LIKE_DID_PATTERN.matcher(attName);
        if (matcher.find()) {
            return "_" + attName;
        }
        matcher = LIKE_INDEX_PATTERN.matcher(attName);
        if (matcher.find()) {
            return "_" + attName;
        }

        return IdsFilter.escapeAttributeName(attName);
    }

    public static String toAttributeName(String fieldName) {
        Matcher matcher = LIKE_DID_PATTERN.matcher(fieldName);
        if (matcher.find()) {
            return fieldName.substring(1);
        }
        matcher = LIKE_INDEX_PATTERN.matcher(fieldName);
        if (matcher.find()) {
            return fieldName.substring(1);
        }

        return fieldName;
    }

    /**
     * The class holding records for this type
     * <p>
     * @return
     */
    @Override
    public Class<SubDocTableRecord> getRecordType() {
        return SubDocTableRecord.class;
    }

    /**
     * {@inheritDoc}
     * <p>
     * @return
     */
    @Override
    public Identity<SubDocTableRecord, Integer> getIdentity() {
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
    public SubDocTable as(String alias) {
        return new SubDocTable(alias, getSchema(), this, getSubDocType());
    }

    /**
     * Rename this table
     * <p>
     * @param name
     * @return
     */
    public SubDocTable rename(String name) {
        return new SubDocTable(name, getSchema(), null, getSubDocType());
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

        public static Identity<SubDocTableRecord, Integer> createIdentity(SubDocTable table) {
            return createIdentity(table, table.didField);
        }
    }
}
