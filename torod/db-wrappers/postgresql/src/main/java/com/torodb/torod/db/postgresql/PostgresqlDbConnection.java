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
package com.torodb.torod.db.postgresql;

import com.google.common.collect.Lists;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.pojos.IndexedAttributes;
import com.torodb.torod.core.pojos.NamedToroIndex;
import com.torodb.torod.core.subdocument.BasicType;
import com.torodb.torod.core.subdocument.SplitDocument;
import com.torodb.torod.db.postgresql.converters.jooq.SubdocValueConverter;
import com.torodb.torod.db.postgresql.converters.jooq.ValueToJooqConverterProvider;
import com.torodb.torod.db.postgresql.meta.CollectionSchema;
import com.torodb.torod.db.postgresql.meta.TorodbMeta;
import com.torodb.torod.db.postgresql.meta.tables.SubDocTable;
import com.torodb.torod.db.sql.AbstractSqlDbConnection;
import com.torodb.torod.db.sql.AutoCloser;
import com.torodb.torod.db.sql.index.NamedDbIndex;
import com.torodb.torod.db.sql.index.UnnamedDbIndex;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.Serializable;
import java.sql.*;
import java.util.*;
import java.util.Comparator;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import org.jooq.*;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

/**
 *
 */
class PostgresqlDbConnection extends AbstractSqlDbConnection {

    static final String SUBDOC_TABLE_PK_COLUMN = "pk";
    static final String SUBDOC_TABLE_DOC_ID_COLUMN = "docId";
    static final String SUBDOC_TABLE_KEYS_COLUMN = "keys";
    private final FieldComparator fieldComparator = new FieldComparator();

    @Inject
    public PostgresqlDbConnection(
            DSLContext dsl,
            TorodbMeta meta) {
        super(dsl, meta);
    }

    @Override
    protected String getCreateIndexQuery(SubDocTable table, Field<?> field, Configuration conf) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE INDEX ON ")
                .append(table.getSchema().getName())
                .append('.')
                .append(table.getName())
                .append(" (")
                .append(field.getName())
                .append(')');

        return sb.toString();
    }

    @Override
    protected String getCreateSubDocTypeTableQuery(SubDocTable table, Configuration conf) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ")
                .append(table.getSchema().getName())
                .append('.')
                .append(table.getName())
                .append('(');

        for (Field field : getFieldIterator(table.fields())) {
            sb
                    .append('"')
                    .append(field.getName())
                    .append("\" ")
                    .append(getSqlType(field, conf));

            sb.append(',');
        }
        if (table.fields().length > 0) {
            sb.delete(sb.length() - 1, sb.length());
        }
        sb.append(')');
        return sb.toString();
    }

    @Override
    public void insertRootDocuments(
            @Nonnull String collection,
            @Nonnull Collection<SplitDocument> docs
    ) throws ImplementationDbException {

        try {
            CollectionSchema colSchema = getMeta().getCollectionSchema(collection);

            Field<Integer> idField = DSL.field("did", SQLDataType.INTEGER.nullable(false));
            Field<Integer> sidField = DSL.field("sid", SQLDataType.INTEGER.nullable(false));


            InsertValuesStep2<Record, Integer, Integer> insertInto = getDsl().insertInto(DSL.tableByName(colSchema.getName(), "root"), idField, sidField);

            for (SplitDocument splitDocument : docs) {
                int structureId = colSchema.getStructuresCache().getOrCreateStructure(splitDocument.getRoot(), getDsl());

                insertInto = insertInto.values(splitDocument.getDocumentId(), structureId);
            }

            insertInto.execute();

        } catch (DataAccessException ex) {
            //TODO: Change exception
            throw new RuntimeException(ex);
        }
    }

    @Override
    public NamedDbIndex createDbIndex(
            NamedToroIndex toroNamedIndex, 
            UnnamedDbIndex dbUnnamedIndex) {
        //TODO: Implement this command
        throw new UnsupportedOperationException("Not supported yet."); 
    }

    @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
    @Override
    protected void dropIndex(NamedDbIndex index) {
        ConnectionProvider connectionProvider
                = getDsl().configuration().connectionProvider();
        Connection connection = connectionProvider.acquire();
        Statement st = null;
        try {
            st = connection.createStatement();
            st.executeUpdate("DROP TABLE " + index.getName());
        } catch (SQLException ex) {
            Logger.getLogger(PostgresqlDbConnection.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            AutoCloser.close(st);
            connectionProvider.release(connection);
        }
    }

    @Override
    public long getDatabaseSize() {
        ConnectionProvider connectionProvider
                = getDsl().configuration().connectionProvider();
        Connection connection = connectionProvider.acquire();
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            ps = connection.prepareStatement("SELECT * from pg_database_size(?)");
            ps.setString(1, getMeta().getDatabaseName());
            rs = ps.executeQuery();
            rs.next();
            return rs.getLong(1);
        }
        catch (SQLException ex) {
            //TODO: Change exception
            throw new RuntimeException(ex);
        }
        finally {
            AutoCloser.close(rs);
            AutoCloser.close(ps);
            connectionProvider.release(connection);
        }
    }

    private String getSqlType(Field<?> field, Configuration conf) {
        if (field.getConverter() != null) {
            SubdocValueConverter arrayConverter
                    = ValueToJooqConverterProvider.getConverter(BasicType.ARRAY);
            SubdocValueConverter twelveBytesConverter
                    = ValueToJooqConverterProvider.getConverter(BasicType.TWELVE_BYTES);

            if (field.getConverter().getClass().equals(arrayConverter.getClass())) {
                return "jsonb";
            }
            if (field.getConverter().getClass().equals(twelveBytesConverter.getClass())) {
                return "twelve_bytes";
            }
        }
        return field.getDataType().getTypeName(conf);
    }

    private Iterable<Field> getFieldIterator(Field[] fields) {
        List<Field> fieldList = Lists.newArrayList(fields);
        Collections.sort(fieldList, fieldComparator);

        return fieldList;
    }

    private static class FieldComparator implements Comparator<Field>, Serializable {

        private static final List<Integer> sqlTypeOrder = Arrays.asList(new Integer[]{
                    java.sql.Types.NULL,
                    java.sql.Types.DOUBLE,
                    java.sql.Types.BIGINT,
                    java.sql.Types.INTEGER,
                    java.sql.Types.FLOAT,
                    java.sql.Types.TIME,
                    java.sql.Types.DATE,
                    java.sql.Types.REAL,
                    java.sql.Types.TINYINT,
                    java.sql.Types.CHAR,
                    java.sql.Types.BIT
                });
        private static final long serialVersionUID = 1L;

        @Override
        public int compare(Field o1, Field o2) {
            if (o1.getName().equals(SubDocTable.DID_COLUMN_NAME)) {
                return -1;
            } else if (o2.getName().equals(SubDocTable.DID_COLUMN_NAME)) {
                return 1;
            }
            if (o1.getName().equals(SubDocTable.INDEX_COLUMN_NAME)) {
                return -1;
            } else if (o2.getName().equals(SubDocTable.INDEX_COLUMN_NAME)) {
                return 1;
            }

            int i1 = sqlTypeOrder.indexOf(o1.getDataType().getSQLType());
            int i2 = sqlTypeOrder.indexOf(o2.getDataType().getSQLType());

            if (i1 == i2) {
                return o1.getName().compareTo(o2.getName());
            }
            if (i1 == -1) {
                return 1;
            }
            if (i2 == -1) {
                return -1;
            }
            return i1 - i2;
        }

    }
}
