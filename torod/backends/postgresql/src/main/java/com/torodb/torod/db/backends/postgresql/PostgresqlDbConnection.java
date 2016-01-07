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
import com.torodb.torod.core.ValueRow;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.exceptions.IllegalPathViewException;
import com.torodb.torod.core.exceptions.UserToroException;
import com.torodb.torod.core.subdocument.BasicType;
import com.torodb.torod.core.subdocument.SplitDocument;
import com.torodb.torod.core.subdocument.structure.DocStructure;
import com.torodb.torod.core.subdocument.values.Value;
import com.torodb.torod.db.backends.DatabaseInterface;
import com.torodb.torod.db.backends.converters.jooq.SubdocValueConverter;
import com.torodb.torod.db.backends.converters.jooq.ValueToJooqConverterProvider;
import com.torodb.torod.db.backends.meta.IndexStorage;
import com.torodb.torod.db.backends.meta.StructuresCache;
import com.torodb.torod.db.backends.meta.TorodbMeta;
import com.torodb.torod.db.backends.sql.AbstractDbConnection;
import com.torodb.torod.db.backends.sql.index.NamedDbIndex;
import com.torodb.torod.db.backends.sql.path.view.DefaultPathViewHandlerCallback;
import com.torodb.torod.db.backends.sql.path.view.PathViewHandler;
import com.torodb.torod.db.backends.sql.utils.SqlWindow;
import com.torodb.torod.db.backends.tables.SubDocTable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.Serializable;
import java.sql.*;
import java.util.Comparator;
import java.util.*;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import org.jooq.*;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;


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
    @SuppressFBWarnings(
            value = "OBL_UNSATISFIED_OBLIGATION",
            justification = "False positive: https://sourceforge.net/p/findbugs/bugs/1021/")
    public long getDatabaseSize() {
        ConnectionProvider connectionProvider
                = getDsl().configuration().connectionProvider();
        Connection connection = connectionProvider.acquire();
        
        try (PreparedStatement ps = connection.prepareStatement("SELECT * from pg_database_size(?)")) {
            ps.setString(1, getMeta().getDatabaseName());
            ResultSet rs = ps.executeQuery();
            rs.next();
            return rs.getLong(1);
        }
        catch (SQLException ex) {
            //TODO: Change exception
            throw new RuntimeException(ex);
        }
        finally {
            connectionProvider.release(connection);
        }
    }

    @Override
    @SuppressFBWarnings(
            value = "OBL_UNSATISFIED_OBLIGATION",
            justification = "False positive: https://sourceforge.net/p/findbugs/bugs/1021/")
    public Long getCollectionSize(String collection) {
        IndexStorage.CollectionSchema colSchema = getMeta().getCollectionSchema(collection);
        
        ConnectionProvider connectionProvider 
                = getDsl().configuration().connectionProvider();
        
        Connection connection = connectionProvider.acquire();

        String query = "SELECT sum(table_size)::bigint "
                + "FROM ("
                + "  SELECT "
                + "    pg_relation_size(pg_catalog.pg_class.oid) as table_size "
                + "  FROM pg_catalog.pg_class "
                + "    JOIN pg_catalog.pg_namespace "
                + "       ON relnamespace = pg_catalog.pg_namespace.oid "
                + "    WHERE pg_catalog.pg_namespace.nspname = ?"
                + ") AS t";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setString(1, colSchema.getName());
            ResultSet rs = ps.executeQuery();
            rs.next();
            return rs.getLong(1);
        }
        catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
        finally {
            connectionProvider.release(connection);
        }
    }

    @SuppressFBWarnings("SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING")
    @Override
    protected void createSchema(String escapedSchemaName) throws SQLException {
        Connection c = getDsl().configuration().connectionProvider().acquire();

        String query = "CREATE SCHEMA IF NOT EXISTS \"" + escapedSchemaName + "\"";
        try (PreparedStatement ps = c.prepareStatement(query)) {
            ps.executeUpdate();
        } finally {
            getDsl().configuration().connectionProvider().release(c);
        }
    }

    @SuppressFBWarnings("SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING")
    @Override
    protected void createStructuresTable(String escapedSchemaName) throws SQLException {
        Connection c = getDsl().configuration().connectionProvider().acquire();

        String query = "CREATE TABLE \"" + escapedSchemaName + "\".structures("
                    + "sid int PRIMARY KEY,"
                    + "_structure jsonb NOT NULL"
                    + ")";

        try (PreparedStatement ps = c.prepareStatement(query)) {
            ps.executeUpdate();
        } finally {
            getDsl().configuration().connectionProvider().release(c);
        }
    }

    @SuppressFBWarnings("SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING")
    @Override
    protected void createRootTable(String escapedSchemaName) throws SQLException {
        Connection c = getDsl().configuration().connectionProvider().acquire();

        String query = "CREATE TABLE \""+ escapedSchemaName + "\".root("
                    + "did int PRIMARY KEY DEFAULT nextval('\"" + escapedSchemaName + "\".root_seq'),"
                    + "sid int NOT NULL"
                    + ")";
        try (PreparedStatement ps = c.prepareStatement(query)) {
            ps.executeUpdate();
        } finally {
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

        String query = "CREATE SEQUENCE "
                    + "\""+ escapedSchemaName +"\".\"" + seqName + "\" "
                    + "MINVALUE 0 START 0";
        try (PreparedStatement ps = c.prepareStatement(query)) {
            ps.executeUpdate();
        } finally {
            getDsl().configuration().connectionProvider().release(c);
        }
    }

    @Override
    @SuppressFBWarnings(
            value = "OBL_UNSATISFIED_OBLIGATION",
            justification = "False positive: https://sourceforge.net/p/findbugs/bugs/1021/")
    public Long getDocumentsSize(String collection) {
        IndexStorage.CollectionSchema colSchema = getMeta().getCollectionSchema(collection);
        
        ConnectionProvider connectionProvider 
                = getDsl().configuration().connectionProvider();
        
        Connection connection = connectionProvider.acquire();
        String query = "SELECT sum(table_size)::bigint from ("
                    + "SELECT pg_relation_size(pg_class.oid) AS table_size "
                    + "FROM pg_class join pg_tables on pg_class.relname = pg_tables.tablename "
                    + "where pg_tables.schemaname = ? "
                    + "   and pg_tables.tablename LIKE 't_%'"
                    + ") as t";
        
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setString(1, colSchema.getName());
            ResultSet rs = ps.executeQuery();
            rs.next();
            
            return rs.getLong(1);
        }
        catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
        finally {
            connectionProvider.release(connection);
        }
    }

    @Override
    public Long getIndexSize(String collection, String index) {
        IndexStorage.CollectionSchema colSchema = getMeta().getCollectionSchema(collection);
        
        ConnectionProvider connectionProvider 
                = getDsl().configuration().connectionProvider();
        
        Connection connection = connectionProvider.acquire();
        
        Set<NamedDbIndex> relatedDbIndexes
                = colSchema.getIndexManager().getRelatedDbIndexes(index);

        String query = "SELECT sum(table_size)::bigint from ("
                + "SELECT pg_relation_size(pg_class.oid) AS table_size "
                + "FROM pg_class join pg_indexes "
                + "  on pg_class.relname = pg_indexes.tablename "
                + "WHERE pg_indexes.schemaname = ? "
                + "  and pg_indexes.indexname = ?"
                + ") as t";

        long result = 0;
        try {
            
            for (NamedDbIndex dbIndex : relatedDbIndexes) {
                try (PreparedStatement ps = connection.prepareStatement(query)) {
                    ps.setString(1, colSchema.getName());
                    ps.setString(2, dbIndex.getName());
                    ResultSet rs = ps.executeQuery();
                    int usedBy = colSchema.getIndexManager().getRelatedToroIndexes(
                            dbIndex.getName()
                    ).size();
                    assert usedBy != 0;
                    rs.next();
                    result += rs.getLong(1) / usedBy;
                }
            }
            return result;
        }
        catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
        finally {
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

    @SuppressFBWarnings(
            value = "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE",
            justification = "It is known that this command is unsafe. We need"
                    + "to improve it as soon as we can")
    @Override
    public Iterator<ValueRow<Value>> select(String query) throws UserToroException {
        Connection connection = getJooqConf().connectionProvider().acquire();
        try {
            try (Statement st = connection.createStatement()) {
                //This is executed to force read only executions
                st.executeUpdate("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
                st.executeUpdate("SET TRANSACTION READ ONLY");
                st.executeUpdate("SET TRANSACTION DEFERRABLE");
                //Once the first query is executed, transacion level is immutable
                ResultSet fakeRS = st.executeQuery("SELECT 1");
                fakeRS.close();


                try (ResultSet rs = st.executeQuery(query)) {
                    return new SqlWindow(rs, getDatabaseInterface().getBasicTypeToSqlType());
                }
            } catch (SQLException ex) {
                //TODO: Change exception
                throw new UserToroException(ex);
            }
        } finally {
            getJooqConf().connectionProvider().release(connection);
        }
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
