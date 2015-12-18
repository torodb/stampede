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
package com.torodb.torod.db.backends.postgresql;

import com.google.common.collect.Lists;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.exceptions.IllegalPathViewException;
import com.torodb.torod.core.subdocument.BasicType;
import com.torodb.torod.core.subdocument.SplitDocument;
import com.torodb.torod.core.subdocument.structure.DocStructure;
import com.torodb.torod.db.backends.DatabaseInterface;
import com.torodb.torod.db.backends.converters.jooq.SubdocValueConverter;
import com.torodb.torod.db.backends.converters.jooq.ValueToJooqConverterProvider;
import com.torodb.torod.db.backends.meta.IndexStorage;
import com.torodb.torod.db.backends.meta.StructuresCache;
import com.torodb.torod.db.backends.meta.TorodbMeta;
import com.torodb.torod.db.backends.sql.AbstractDbConnection;
import com.torodb.torod.db.backends.sql.AutoCloser;
import com.torodb.torod.db.backends.sql.index.NamedDbIndex;
import com.torodb.torod.db.backends.sql.path.view.DefaultPathViewHandlerCallback;
import com.torodb.torod.db.backends.sql.path.view.PathViewHandler;
import com.torodb.torod.db.backends.tables.SubDocTable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.*;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.Comparator;


/**
 *
 */
class PostgresqlDbConnection extends AbstractDbConnection {

    static final String SUBDOC_TABLE_PK_COLUMN = "pk";
    static final String SUBDOC_TABLE_DOC_ID_COLUMN = "docId";
    static final String SUBDOC_TABLE_KEYS_COLUMN = "keys";
    private final FieldComparator fieldComparator = new FieldComparator();
    private final MyStructureListener listener = new MyStructureListener();

    @Inject
    public PostgresqlDbConnection(
            DSLContext dsl,
            TorodbMeta meta,
            DatabaseInterface databaseInterface
    ) {
        super(dsl, meta, databaseInterface);
    }

    @Override
    protected String getCreateIndexQuery(SubDocTable table, Field<?> field, Configuration conf) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE INDEX ON \"")
                .append(table.getSchema().getName())
                .append("\".\"")
                .append(table.getName())
                .append("\" (\"")
                .append(field.getName())
                .append("\")");

        return sb.toString();
    }

    @Override
    protected String getCreateSubDocTypeTableQuery(SubDocTable table, Configuration conf) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE \"")
                .append(table.getSchema().getName())
                .append("\".\"")
                .append(table.getName())
                .append("\"(");

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
            IndexStorage.CollectionSchema colSchema = getMeta().getCollectionSchema(collection);

            Field<Integer> idField = DSL.field("did", SQLDataType.INTEGER.nullable(false));
            Field<Integer> sidField = DSL.field("sid", SQLDataType.INTEGER.nullable(false));


            InsertValuesStep2<Record, Integer, Integer> insertInto = 
                    getDsl()
                    .insertInto(
                            DSL.table(DSL.name(colSchema.getName(), "root")), 
                            idField, 
                            sidField
                    );

            for (SplitDocument splitDocument : docs) {
                int structureId = colSchema.getStructuresCache().getOrCreateStructure(
                        splitDocument.getRoot(), 
                        getDsl(),
                        listener
                );

                insertInto = insertInto.values(splitDocument.getDocumentId(), structureId);
            }

            insertInto.execute();

        } catch (DataAccessException ex) {
            //TODO: Change exception
            throw new RuntimeException(ex);
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

    @Override
    public Long getCollectionSize(String collection) {
        IndexStorage.CollectionSchema colSchema = getMeta().getCollectionSchema(collection);
        
        ConnectionProvider connectionProvider 
                = getDsl().configuration().connectionProvider();
        
        Connection connection = connectionProvider.acquire();
        PreparedStatement ps = null;
        ResultSet rs = null;
        
        try {
            ps = connection.prepareStatement("SELECT sum(table_size)::bigint " 
                + "FROM ("
                + "  SELECT "
                + "    pg_relation_size(pg_catalog.pg_class.oid) as table_size "
                + "  FROM pg_catalog.pg_class "
                + "    JOIN pg_catalog.pg_namespace "
                + "       ON relnamespace = pg_catalog.pg_namespace.oid "
                + "    WHERE pg_catalog.pg_namespace.nspname = ?"
                + ") AS t"
            );
            ps.setString(1, colSchema.getName());
            rs = ps.executeQuery();
            rs.next();
            return rs.getLong(1);
        }
        catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
        finally {
            AutoCloser.close(rs);
            AutoCloser.close(ps);
            connectionProvider.release(connection);
        }
    }

    @SuppressFBWarnings("SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING")
    @Override
    protected void createSchema(String escapedSchemaName) throws SQLException {
        Connection c = getDsl().configuration().connectionProvider().acquire();

        PreparedStatement ps = null;
        try {
            ps = c.prepareStatement("CREATE SCHEMA IF NOT EXISTS \"" + escapedSchemaName + "\"");
            ps.executeUpdate();
        } finally {
            AutoCloser.close(ps);
            getDsl().configuration().connectionProvider().release(c);
        }
    }

    @SuppressFBWarnings("SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING")
    @Override
    protected void createStructuresTable(String escapedSchemaName) throws SQLException {
        Connection c = getDsl().configuration().connectionProvider().acquire();

        PreparedStatement ps = null;
        try {
            ps = c.prepareStatement("CREATE TABLE \"" + escapedSchemaName + "\".structures("
                    + "sid int PRIMARY KEY,"
                    + "_structure jsonb NOT NULL"
                    + ")");
            ps.executeUpdate();
        } finally {
            AutoCloser.close(ps);
            getDsl().configuration().connectionProvider().release(c);
        }
    }

    @SuppressFBWarnings("SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING")
    @Override
    protected void createRootTable(String escapedSchemaName) throws SQLException {
        Connection c = getDsl().configuration().connectionProvider().acquire();

        PreparedStatement ps = null;
        try {
            ps = c.prepareStatement("CREATE TABLE \""+ escapedSchemaName + "\".root("
                    + "did int PRIMARY KEY DEFAULT nextval('\"" + escapedSchemaName + "\".root_seq'),"
                    + "sid int NOT NULL"
                    + ")");
            ps.executeUpdate();
        } finally {
            AutoCloser.close(ps);
            getDsl().configuration().connectionProvider().release(c);
        }
    }

    @Override
    protected String getRootSeqName() {
        return "root_seq";
    }

    @SuppressFBWarnings("SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING")
    @Override
    protected void createSequence(String escapedSchemaName, String seqName) throws SQLException {
        Connection c = getDsl().configuration().connectionProvider().acquire();

        PreparedStatement ps = null;
        try {
            ps = c.prepareStatement("CREATE SEQUENCE "
                    + "\""+ escapedSchemaName +"\".\"" + seqName + "\" "
                    + "MINVALUE 0 START 0");
            ps.executeUpdate();
        } finally {
            AutoCloser.close(ps);
            getDsl().configuration().connectionProvider().release(c);
        }
    }

    @Override
    public Long getDocumentsSize(String collection) {
        IndexStorage.CollectionSchema colSchema = getMeta().getCollectionSchema(collection);
        
        ConnectionProvider connectionProvider 
                = getDsl().configuration().connectionProvider();
        
        Connection connection = connectionProvider.acquire();
        PreparedStatement ps = null;
        ResultSet rs = null;
        
        try {
            ps = connection.prepareStatement(
                    "SELECT sum(table_size)::bigint from ("
                    + "SELECT pg_relation_size(pg_class.oid) AS table_size "
                    + "FROM pg_class join pg_tables on pg_class.relname = pg_tables.tablename "
                    + "where pg_tables.schemaname = ? "
                    + "   and pg_tables.tablename LIKE 't_%'"
                    + ") as t");
            ps.setString(1, colSchema.getName());
            rs = ps.executeQuery();
            rs.next();
            
            return rs.getLong(1);
        }
        catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
        finally {
            AutoCloser.close(rs);
            AutoCloser.close(ps);
            connectionProvider.release(connection);
        }
    }

    @Override
    public Long getIndexSize(String collection, String index) {
        IndexStorage.CollectionSchema colSchema = getMeta().getCollectionSchema(collection);
        
        ConnectionProvider connectionProvider 
                = getDsl().configuration().connectionProvider();
        
        Connection connection = connectionProvider.acquire();
        PreparedStatement ps = null;
        
        Set<NamedDbIndex> relatedDbIndexes
                = colSchema.getIndexManager().getRelatedDbIndexes(index);
        
        long result = 0;
        ResultSet rs = null;
        try {
            
            for (NamedDbIndex dbIndex : relatedDbIndexes) {
                ps = connection.prepareStatement(
                    "SELECT sum(table_size)::bigint from ("
                    + "SELECT pg_relation_size(pg_class.oid) AS table_size "
                    + "FROM pg_class join pg_indexes "
                    + "  on pg_class.relname = pg_indexes.tablename "
                    + "WHERE pg_indexes.schemaname = ? "
                    + "  and pg_indexes.indexname = ?"
                    + ") as t");
                
                ps.setString(1, colSchema.getName());
                ps.setString(2, dbIndex.getName());
                rs = ps.executeQuery();
                int usedBy = colSchema.getIndexManager().getRelatedToroIndexes(
                        dbIndex.getName()
                ).size();
                assert usedBy != 0;
                rs.next();
                result += rs.getLong(1) / usedBy;
            }
            return result;
        }
        catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
        finally {
            AutoCloser.close(rs);
            AutoCloser.close(ps);
            connectionProvider.release(connection);
        }
    }

    @Override
    public Integer createPathViews(String collection) throws IllegalPathViewException {
        PathViewHandler.Callback callback = new DefaultPathViewHandlerCallback(getDsl());
        PathViewHandler handler = new PathViewHandler(getMeta(), callback);

        return handler.createPathViews(collection);
    }

    @Override
    public void dropPathViews(String collection) throws
            IllegalPathViewException {
        PathViewHandler.Callback callback = new DefaultPathViewHandlerCallback(getDsl());
        PathViewHandler handler = new PathViewHandler(getMeta(), callback);

        handler.dropPathViews(collection);
    }

    private String getSqlType(Field<?> field, Configuration conf) {
        if (field.getConverter() != null) {
            SubdocValueConverter arrayConverter
                    = ValueToJooqConverterProvider.getConverter(BasicType.ARRAY);
            if (field.getConverter().getClass().equals(arrayConverter.getClass())) {
            	return "jsonb";
            }
            SubdocValueConverter twelveBytesConverter
                    = ValueToJooqConverterProvider.getConverter(BasicType.TWELVE_BYTES);
            if (field.getConverter().getClass().equals(twelveBytesConverter.getClass())) {
            	return "torodb.twelve_bytes";
            }
            SubdocValueConverter patternConverter
                    = ValueToJooqConverterProvider.getConverter(BasicType.PATTERN);
            if (field.getConverter().getClass().equals(patternConverter.getClass())) {
                return "torodb.torodb_pattern";
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
    
    private class MyStructureListener implements StructuresCache.NewStructureListener {

        @Override
        public void eventNewStructure(IndexStorage.CollectionSchema colSchema, DocStructure newStructure) {
            colSchema.getIndexManager().newStructureDetected(
                    newStructure, 
                    PostgresqlDbConnection.this
            );
        }
        
    }
}
