package com.torodb.backend.derby;

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

import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Configuration;
import org.jooq.ConnectionProvider;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.torodb.backend.DatabaseInterface;
import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.backend.converters.jooq.ValueToJooqDataTypeProvider;
import com.torodb.backend.derby.converters.DerbyKVTypeToSqlType;
import com.torodb.backend.derby.converters.DerbyValueToCopyConverter;
import com.torodb.backend.derby.converters.jooq.DerbyValueToJooqDataTypeProvider;
import com.torodb.backend.derby.tables.DerbyMetaCollectionTable;
import com.torodb.backend.derby.tables.DerbyMetaDatabaseTable;
import com.torodb.backend.derby.tables.DerbyMetaDocPartTable;
import com.torodb.backend.derby.tables.DerbyMetaFieldTable;
import com.torodb.backend.meta.TorodbSchema;
import com.torodb.backend.mocks.KVTypeToSqlType;
import com.torodb.backend.mocks.RetryTransactionException;
import com.torodb.backend.mocks.ToroImplementationException;
import com.torodb.backend.mocks.ToroRuntimeException;
import com.torodb.backend.sql.index.NamedDbIndex;
import com.torodb.backend.tables.DocPartTable;
import com.torodb.backend.tables.MetaCollectionTable;
import com.torodb.backend.tables.MetaDatabaseTable;
import com.torodb.backend.tables.MetaDocPartTable;
import com.torodb.backend.tables.MetaFieldTable;
import com.torodb.core.TableRef;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.d2r.DocPartRow;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.kvdocument.values.KVValue;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 *
 */
@Singleton
public class DerbyDatabaseInterface implements DatabaseInterface {
    private static final Logger LOGGER
        = LogManager.getLogger(DerbyDatabaseInterface.class);

    private static final long serialVersionUID = 484638503;

    private static final String[] RESTRICTED_SCHEMA_NAMES = new String[] {
            TorodbSchema.TORODB_SCHEMA,
            "NULLID",
            "SQLJ",
            "SYS",
            "SYSCAT",
            "SYSCS_DIAG",
            "SYSCS_UTIL",
            "SYSFUN",
            "SYSIBM",
            "SYSPROC",
            "SYSSTAT",
    };
    {
        Arrays.sort(RESTRICTED_SCHEMA_NAMES);
    }

    private static final String[] RESTRICTED_COLUMN_NAMES = new String[] {
    };
    {
        Arrays.sort(RESTRICTED_COLUMN_NAMES);
    }
    
    private final ValueToJooqDataTypeProvider valueToJooqDataTypeProvider;
    private final KVTypeToSqlType kVTypeToSqlType;
    private final FieldComparator fieldComparator = new FieldComparator();

    @Inject
    public DerbyDatabaseInterface() {
        this.valueToJooqDataTypeProvider = DerbyValueToJooqDataTypeProvider.getInstance();
        this.kVTypeToSqlType = new DerbyKVTypeToSqlType();
    }

    private void readObject(java.io.ObjectInputStream stream)
            throws IOException, ClassNotFoundException {
        //TODO: Try to remove make DatabaseInterface not serializable
        stream.defaultReadObject();
    }

    @Nonnull
    @Override
    public DerbyMetaDatabaseTable getMetaDatabaseTable() {
        return new DerbyMetaDatabaseTable();
    }

    @Nonnull
    @Override
    public DerbyMetaCollectionTable getMetaCollectionTable() {
        return new DerbyMetaCollectionTable();
    }

    @Nonnull
    @Override
    public DerbyMetaDocPartTable getMetaDocPartTable() {
        return new DerbyMetaDocPartTable();
    }

    @Nonnull
    @Override
    public DerbyMetaFieldTable getMetaFieldTable() {
        return new DerbyMetaFieldTable();
    }

    @Override
    public @Nonnull ValueToJooqDataTypeProvider getValueToJooqDataTypeProvider() {
        return valueToJooqDataTypeProvider;
    }

    @Override
    public String createIndexStatement(Configuration conf, String schemaName, String tableName, String fieldName) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE INDEX ON \"")
                .append(schemaName)
                .append("\".\"")
                .append(tableName)
                .append("\" (\"")
                .append(fieldName)
                .append("\")");

        return sb.toString();
    }

    @Override
    public String createDocPartTableStatement(Configuration conf, String schemaName, String tableName, Collection<Field<?>> fields) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ")
                .append(fullTableName(schemaName, tableName))
                .append(" (");

        for (Field<?> field : getFieldIterator(fields)) {
            sb
                    .append('"')
                    .append(field.getName())
                    .append("\" ")
                    .append(getSqlType(field, conf));

            sb.append(',');
        }
        if (fields.size() > 0) {
            sb.delete(sb.length() - 1, sb.length());
        }
        sb.append(')');
        return sb.toString();
    }

    @Override
    public String addColumnsToDocPartTableStatement(Configuration conf, String schemaName, String tableName, Collection<Field<?>> fields) {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ")
                .append(fullTableName(schemaName, tableName));

        for (Field<?> field : getFieldIterator(fields)) {
            sb
                    .append(" ADD COLUMN \"")
                    .append(field.getName())
                    .append("\" ")
                    .append(getSqlType(field, conf));

            sb.append(',');
        }
        if (fields.size() > 0) {
            sb.delete(sb.length() - 1, sb.length());
        }
        sb.append(')');
        return sb.toString();
    }

    private String getSqlType(Field<?> field, Configuration conf) {
        return field.getDataType().getCastTypeName(conf);
    }

    private Iterable<Field<?>> getFieldIterator(Iterable<Field<?>> fields) {
        List<Field<?>> fieldList = Lists.newArrayList(fields);
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
                    java.sql.Types.BIT,
                    java.sql.Types.BINARY
                });
        private static final long serialVersionUID = 1L;

        @Override
        public int compare(Field o1, Field o2) {
            if (o1.getName().equals(DocPartTable.DID_COLUMN_NAME)) {
                return -1;
            } else if (o2.getName().equals(DocPartTable.DID_COLUMN_NAME)) {
                return 1;
            }
            if (o1.getName().equals(DocPartTable.RID_COLUMN_NAME)) {
                return -1;
            } else if (o2.getName().equals(DocPartTable.RID_COLUMN_NAME)) {
                return 1;
            }
            if (o1.getName().equals(DocPartTable.PID_COLUMN_NAME)) {
                return -1;
            } else if (o2.getName().equals(DocPartTable.PID_COLUMN_NAME)) {
                return 1;
            }
            if (o1.getName().equals(DocPartTable.SEQ_COLUMN_NAME)) {
                return -1;
            } else if (o2.getName().equals(DocPartTable.SEQ_COLUMN_NAME)) {
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

    @Override
    public @Nonnull String escapeSchemaName(@Nonnull String collection) throws IllegalArgumentException {
        return filter(collection);
    }

    @Override
    public @Nonnull String escapeAttributeName(@Nonnull String attributeName) throws IllegalArgumentException {
        return filter(attributeName);
    }

    @Override
    public @Nonnull String escapeIndexName(@Nonnull String indexName) throws IllegalArgumentException {
        return filter(indexName);
    }

    private static String filter(String str) {
        if (str.length() > 128) {
            throw new IllegalArgumentException(str + " is too long to be a "
                    + "valid Derby name. By default names must be shorter "
                    + "than 129, but it has " + str.length() + " characters");
        }
        Pattern quotesPattern = Pattern.compile("(\"+)");
        Matcher matcher = quotesPattern.matcher(str);
        while (matcher.find()) {
            if (((matcher.end() - matcher.start()) & 1) == 1) { //lenght is uneven
                throw new IllegalArgumentException("The name '" + str + "' is"
                        + "illegal because contains an open quote at " + matcher.start());
            }
        }

        return str;
    }

    @Override
    public boolean isRestrictedSchemaName(@Nonnull String schemaName) {
        return Arrays.binarySearch(RESTRICTED_SCHEMA_NAMES, schemaName) >= 0;
    }
    
    @Override
    public boolean isRestrictedColumnName(@Nonnull String columnName) {
        return Arrays.binarySearch(RESTRICTED_COLUMN_NAMES, columnName) >= 0;
    }

    @Override
    @Nonnull
    public ResultSet getColumns(DatabaseMetaData metadata, String schemaName, String tableName) throws SQLException {
        return metadata.getColumns("%", schemaName, tableName, null);
    }
    
    @Override
    @Nonnull
    public ResultSet getIndexes(DatabaseMetaData metadata, String schemaName, String tableName) throws SQLException {
        return metadata.getIndexInfo(
                "%",
                schemaName,
                tableName,
                false,
                false
        );
    }

    @Override
    public int getIntColumnType(ResultSet columns) throws SQLException {
        return columns.getInt("DATA_TYPE");
    }

    @Override
    public String getStringColumnType(ResultSet columns) throws SQLException {
        return columns.getString("TYPE_NAME");
    }

    @Override
    public KVTypeToSqlType getKVTypeToSqlType() {
        return kVTypeToSqlType;
    }

    @Override
    public @Nonnull String createSchemaStatement(@Nonnull String schemaName) {
        return new StringBuilder().append("CREATE SCHEMA ").append("\"").append(schemaName).append("\"").toString();
    }

    @Override
    public @Nonnull String createMetaDatabaseTableStatement(@Nonnull String schemaName, @Nonnull String tableName) {
        return new StringBuilder()
                .append("CREATE TABLE ")
                .append(fullTableName(schemaName, tableName))
                .append(" (")
                .append('"').append(MetaDatabaseTable.TableFields.NAME.toString()).append('"').append("             varchar(4192)     PRIMARY KEY     ,")
                .append('"').append(MetaDatabaseTable.TableFields.IDENTIFIER.toString()).append('"').append("       varchar(128)      NOT NULL UNIQUE ")
                .append(")")
                .toString();
    }

    @Override
    public @Nonnull String createMetaCollectionTableStatement(@Nonnull String schemaName, @Nonnull String tableName) {
        return new StringBuilder()
                .append("CREATE TABLE ")
                .append(fullTableName(schemaName, tableName))
                .append(" (")
                .append('"').append(MetaCollectionTable.TableFields.DATABASE.toString()).append('"').append("         varchar(4192)     NOT NULL        ,")
                .append('"').append(MetaCollectionTable.TableFields.NAME.toString()).append('"').append("             varchar(4192)     NOT NULL        ,")
                .append("    PRIMARY KEY (").append('"').append(MetaCollectionTable.TableFields.DATABASE.toString()).append('"').append(",")
                    .append('"').append(MetaCollectionTable.TableFields.NAME.toString()).append('"').append(")")
                .append(")")
                .toString();
    }

    @Override
    public @Nonnull String createMetaDocPartTableStatement(@Nonnull String schemaName, @Nonnull String tableName) {
        return new StringBuilder()
                .append("CREATE TABLE ")
                .append(fullTableName(schemaName, tableName))
                .append(" (")
                .append('"').append(MetaDocPartTable.TableFields.DATABASE.toString()).append('"').append("         varchar(4192)     NOT NULL        ,")
                .append('"').append(MetaDocPartTable.TableFields.COLLECTION.toString()).append('"').append("       varchar(4192)     NOT NULL        ,")
                .append('"').append(MetaDocPartTable.TableFields.TABLE_REF.toString()).append('"').append("        varchar(32672)    NOT NULL        ,")
                .append('"').append(MetaDocPartTable.TableFields.IDENTIFIER.toString()).append('"').append("       varchar(128)      NOT NULL        ,")
                .append('"').append(MetaDocPartTable.TableFields.LAST_RID.toString()).append('"').append("         integer           NOT NULL        ,")
                .append("    PRIMARY KEY (").append('"').append(MetaDocPartTable.TableFields.DATABASE.toString()).append('"').append(",")
                    .append('"').append(MetaDocPartTable.TableFields.COLLECTION.toString()).append('"').append(",")
                    .append('"').append(MetaDocPartTable.TableFields.TABLE_REF.toString()).append('"').append("),")
                .append("    UNIQUE (").append('"').append(MetaDocPartTable.TableFields.DATABASE.toString()).append('"').append(",")
                    .append('"').append(MetaDocPartTable.TableFields.IDENTIFIER.toString()).append('"').append(")")
                .append(")")
                .toString();
    }

    @Override
    public @Nonnull String createMetaFieldTableStatement(@Nonnull String schemaName, @Nonnull String tableName) {
        return new StringBuilder()
                .append("CREATE TABLE ")
                .append(fullTableName(schemaName, tableName))
                .append(" (")
                .append('"').append(MetaFieldTable.TableFields.DATABASE.toString()).append('"').append("         varchar(4192)    NOT NULL        ,")
                .append('"').append(MetaFieldTable.TableFields.COLLECTION.toString()).append('"').append("       varchar(4192)    NOT NULL        ,")
                .append('"').append(MetaFieldTable.TableFields.TABLE_REF.toString()).append('"').append("        varchar(32672)   NOT NULL        ,")
                .append('"').append(MetaFieldTable.TableFields.NAME.toString()).append('"').append("             varchar(4192)    NOT NULL        ,")
                .append('"').append(MetaFieldTable.TableFields.IDENTIFIER.toString()).append('"').append("       varchar(128)     NOT NULL        ,")
                .append('"').append(MetaFieldTable.TableFields.TYPE.toString()).append('"').append("             varchar(128)     NOT NULL        ,")
                .append("    PRIMARY KEY (").append('"').append(MetaFieldTable.TableFields.DATABASE.toString()).append('"').append(",")
                .append('"').append(MetaFieldTable.TableFields.COLLECTION.toString()).append('"').append(",")
                .append('"').append(MetaFieldTable.TableFields.TABLE_REF.toString()).append('"').append(",")
                    .append('"').append(MetaFieldTable.TableFields.NAME.toString()).append('"').append("),")
                .append("    UNIQUE (").append('"').append(MetaFieldTable.TableFields.DATABASE.toString()).append('"').append(",")
                    .append('"').append(MetaFieldTable.TableFields.COLLECTION.toString()).append('"').append(",")
                    .append('"').append(MetaFieldTable.TableFields.TABLE_REF.toString()).append('"').append(",")
                    .append('"').append(MetaFieldTable.TableFields.IDENTIFIER.toString()).append('"').append(")")
                .append(")")
                .toString();
    }

    @Override
    public @Nonnull String createMetaIndexesTableStatement(
            @Nonnull String schemaName, @Nonnull String tableName, @Nonnull String indexNameColumn, @Nonnull String indexOptionsColumn
    ) {
        return new StringBuilder()
                .append("CREATE TABLE ")
                .append(fullTableName(schemaName, tableName))
                .append(" (")
                .append('"').append(indexNameColumn).append('"').append("       varchar(4192)     PRIMARY KEY,")
                .append('"').append(indexOptionsColumn).append('"').append("    varchar(23672)    NOT NULL")
                .append(")")
                .toString();
    }

    private static @Nonnull StringBuilder fullTableName(@Nonnull String schemaName, @Nonnull String tableName) {
        return new StringBuilder()
                .append('"').append(schemaName).append('"')
                .append(".")
                .append('"').append(tableName).append('"');
    }

    @Override
    public @Nonnull String arrayUnnestParametrizedSelectStatement() {
        return "SELECT unnest(?)";
    }

    @Override
    public @Nonnull String deleteDidsStatement(
            @Nonnull String schemaName, @Nonnull String tableName,
            @Nonnull String didColumnName
    ) {
        return new StringBuilder()
                .append("DELETE FROM ")
                .append(fullTableName(schemaName, tableName))
                .append(" WHERE (")
                    .append(fullTableName(schemaName, tableName))
                    .append(".").append(didColumnName)
                    .append(" IN (")
                        .append(arrayUnnestParametrizedSelectStatement())
                    .append(")")
                .append(")")
                .toString();
    }

    @Override
    public void setDeleteDidsStatementParameters(@Nonnull PreparedStatement ps,
            @Nonnull Collection<Integer> dids) throws SQLException {
        Connection connection = ps.getConnection();
        
        final int maxInArray = (2 << 15) - 1; // = 2^16 -1 = 65535

        Integer[] didsToDelete = dids.toArray(new Integer[dids.size()]);

        int i = 0;
        while (i < didsToDelete.length) {
            int toIndex = Math.min(i + maxInArray, didsToDelete.length);
            Integer[] subDids
                    = Arrays.copyOfRange(didsToDelete, i, toIndex);

            Array arr
                = connection.createArrayOf("integer", subDids);
            ps.setArray(1, arr);
            ps.addBatch();

            i = toIndex;
        }
    }

    @Override
    public @Nonnull String dropSchemaStatement(@Nonnull String schemaName) {
        return new StringBuilder()
                .append("DROP SCHEMA ")
                .append("\"").append(schemaName).append("\"")
                .append(" CASCADE")
                .toString();
    }

    @Nonnull
    @Override
    public String findDocsSelectStatement() {
        return new StringBuilder()
                .append("SELECT did, typeid, index, _json")
                .append(" FROM torodb.find_docs(?, ?, ?) ORDER BY did ASC")
                .toString();
    }

    @Nonnull
    @Override
    public ResultSet getFindDocsSelectStatementResultSet(PreparedStatement ps) throws SQLException {
        return ps.executeQuery();
    }

    private static class PostgresSQLFindDocsSelectStatementRow implements FindDocsSelectStatementRow {
        private final int docid;
        private final Integer rid;
        private final Integer pid;
        private final Integer seq;
        private final String json;
        
        private PostgresSQLFindDocsSelectStatementRow(ResultSet rs) throws SQLException {
            docid = rs.getInt(1);
            Object rid = rs.getObject(2);
            Object pid = rs.getObject(3);
            Object seq = rs.getObject(4);
            json = rs.getString(5);
            assert rid == null || rid instanceof Integer;
            assert pid == null || pid instanceof Integer;
            assert seq == null || seq instanceof Integer;
            assert json != null;

            this.rid = (Integer) rid;
            this.pid = (Integer) pid;
            this.seq = (Integer) seq;
        }
        
        @Override
        public int getDocId() {
            return docid;
        }

        @Override
        public Integer getRowId() {
            return rid;
        }

        @Override
        public Integer getParentRowId() {
            return pid;
        }

        @Override
        public Integer getSequence() {
            return seq;
        }

        @Override
        public String getJson() {
            return json;
        }

        @Override
        public boolean isRoot() {
            return rid == null;
        }

        @Override
        public boolean isObject() {
            return seq == null;
        }
    };

    @Nonnull
    @Override
    public FindDocsSelectStatementRow getFindDocsSelectStatementRow(ResultSet rs) throws SQLException {
        return new PostgresSQLFindDocsSelectStatementRow(rs);
    }

    @Override
    public void setFindDocsSelectStatementParameters(String schema, Integer[] requestedDocs,
            String[] paths, Connection connection, PreparedStatement ps) throws SQLException {
        ps.setString(1, schema);

        ps.setArray(2, connection.createArrayOf("integer", requestedDocs));

        ps.setArray(3, connection.createArrayOf("text", paths));
    }

    @Nonnull
    @Override
    public String createIndexStatement(
            @Nonnull String fullIndexName, @Nonnull String tableSchema, @Nonnull String tableName,
            @Nonnull String tableColumnName, boolean ascending
    ) {
        return new StringBuilder()
                .append("CREATE INDEX ")
                .append("\"").append(fullIndexName).append("\"")
                .append(" ON ")
                .append("\"").append(tableSchema).append("\"")
                .append(".")
                .append("\"").append(tableName).append("\"")
                .append(" (")
                    .append("\"").append(tableColumnName).append("\"")
                    .append(" ").append(ascending ? "ASC" : "DESC")
                .append(")")
                .toString();
    }

    @Nonnull
    @Override
    public String dropIndexStatement(@Nonnull String schemaName, @Nonnull String indexName) {
        return new StringBuilder()
                .append("DROP INDEX ")
                .append("\"").append(schemaName).append("\"")
                .append(".")
                .append("\"").append(indexName).append("\"")
                .toString();
    }

    @Override
    @SuppressFBWarnings(
            value = "OBL_UNSATISFIED_OBLIGATION",
            justification = "False positive: https://sourceforge.net/p/findbugs/bugs/1021/")
    public long getDatabaseSize(
            @Nonnull DSLContext dsl,
            @Nonnull String databaseName
            ) {
        ConnectionProvider connectionProvider
                = dsl.configuration().connectionProvider();
        Connection connection = connectionProvider.acquire();
        
        try (PreparedStatement ps = connection.prepareStatement("SELECT * from pg_database_size(?)")) {
            ps.setString(1, databaseName);
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
    public Long getCollectionSize(
            @Nonnull DSLContext dsl,
            @Nonnull String schema,
            @Nonnull String collection
            ) {
        ConnectionProvider connectionProvider 
                = dsl.configuration().connectionProvider();
        
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
            ps.setString(1, schema);
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
    protected void createSchema(
            DSLContext dsl,
            String escapedSchemaName
            ) throws SQLException {
        Connection c = dsl.configuration().connectionProvider().acquire();

        String query = "CREATE SCHEMA IF NOT EXISTS \"" + escapedSchemaName + "\"";
        try (PreparedStatement ps = c.prepareStatement(query)) {
            ps.executeUpdate();
        } finally {
            dsl.configuration().connectionProvider().release(c);
        }
    }

    @SuppressFBWarnings("SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING")
    protected void createRootTable(
            DSLContext dsl,
            String escapedSchemaName
            ) throws SQLException {
        Connection c = dsl.configuration().connectionProvider().acquire();

        String query = "CREATE TABLE \""+ escapedSchemaName + "\".root("
                    + "did int PRIMARY KEY DEFAULT nextval('\"" + escapedSchemaName + "\".root_seq'),"
                    + "sid int NOT NULL"
                    + ")";
        try (PreparedStatement ps = c.prepareStatement(query)) {
            ps.executeUpdate();
        } finally {
            dsl.configuration().connectionProvider().release(c);
        }
    }

    @SuppressFBWarnings("SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING")
    protected void createSequence(
            DSLContext dsl,
            String escapedSchemaName, String seqName
            ) throws SQLException {
        Connection c = dsl.configuration().connectionProvider().acquire();

        String query = "CREATE SEQUENCE "
                    + "\""+ escapedSchemaName +"\".\"" + seqName + "\" "
                    + "MINVALUE 0 START 0";
        try (PreparedStatement ps = c.prepareStatement(query)) {
            ps.executeUpdate();
        } finally {
            dsl.configuration().connectionProvider().release(c);
        }
    }

    @Override
    @SuppressFBWarnings(
            value = "OBL_UNSATISFIED_OBLIGATION",
            justification = "False positive: https://sourceforge.net/p/findbugs/bugs/1021/")
    public Long getDocumentsSize(
            @Nonnull DSLContext dsl,
            @Nonnull String schema,
            String collection
            ) {
        ConnectionProvider connectionProvider 
                = dsl.configuration().connectionProvider();
        
        Connection connection = connectionProvider.acquire();
        String query = "SELECT sum(table_size)::bigint from ("
                    + "SELECT pg_relation_size(pg_class.oid) AS table_size "
                    + "FROM pg_class join pg_tables on pg_class.relname = pg_tables.tablename "
                    + "where pg_tables.schemaname = ? "
                    + "   and pg_tables.tablename LIKE 't_%'"
                    + ") as t";
        
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setString(1, schema);
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
    public Long getIndexSize(
            @Nonnull DSLContext dsl,
            @Nonnull String schema,
            String collection, String index,
            Set<NamedDbIndex> relatedDbIndexes,
            Map<String, Integer> relatedToroIndexes
            ) {
        ConnectionProvider connectionProvider 
                = dsl.configuration().connectionProvider();
        
        Connection connection = connectionProvider.acquire();
        
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
                    ps.setString(1, schema);
                    ps.setString(2, dbIndex.getName());
                    ResultSet rs = ps.executeQuery();
                    assert relatedToroIndexes.containsKey(dbIndex.getName());
                    int usedBy = relatedToroIndexes.get(dbIndex.getName());
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
    public void insertPathDocuments(DSLContext dsl, String schemaName, DocPartData docPartData) throws RetryTransactionException {
        Preconditions.checkArgument(docPartData.rowCount() != 0, "Called insert with 0 documents");
        
        Connection connection = dsl.configuration().connectionProvider().acquire();
        try {
            int maxCappedSize = 10;
            int cappedSize = Math.min(docPartData.rowCount(), maxCappedSize);

            if (cappedSize < maxCappedSize) { //there are not enough elements on the insert => fallback
                LOGGER.debug(
                        "The insert window is not big enough to use copy (the "
                                + "limit is {}, the real size is {}).",
                        maxCappedSize,
                        cappedSize
                );
                standardInsertPathDocuments(dsl, schemaName, docPartData);
            }
            else {
                if (!connection.isWrapperFor(PGConnection.class)) {
                    LOGGER.warn("It was impossible to use the Derby way to "
                            + "insert documents. Inserting using the standard "
                            + "implementation");
                    standardInsertPathDocuments(dsl, schemaName, docPartData);
                }
                else {
                    copyInsertPathDocuments(
                            connection.unwrap(PGConnection.class),
                            schemaName,
                            docPartData
                    );
                }
            }
        } catch (DataAccessException ex) {
            handleRetryException(Context.insert, ex);
            throw new ToroRuntimeException(ex);
        } catch (SQLException ex) {
            handleRetryException(Context.insert, ex);
            throw new ToroImplementationException(ex);
        } catch (IOException ex) {
            throw new ToroRuntimeException(ex);
        } finally {
            dsl.configuration().connectionProvider().release(connection);
        }
    }

    protected void standardInsertPathDocuments(DSLContext dsl, String schemaName, DocPartData docPartData) {
        final int maxBatchSize = 1000;
        final StringBuilder insertStatementBuilder = new StringBuilder(2048);
        final StringBuilder insertStatementHeaderBuilder = new StringBuilder(2048);
        int docCounter = 0;
        MetaDocPart metaDocPart = docPartData.getMetaDocPart();
        insertStatementHeaderBuilder.append("INSERT INTO \"")
            .append(schemaName)
            .append("\".\"")
            .append(metaDocPart.getIdentifier())
            .append("\" (");
        
        Iterator<MetaField> metaFieldIterator = docPartData.orderedMetaFieldIterator();
        while (metaFieldIterator.hasNext()) {
            MetaField metaField = metaFieldIterator.next();
            insertStatementHeaderBuilder.append("\"")
                .append(metaField.getIdentifier())
                .append("\",")
                .append(",");
        }
        insertStatementHeaderBuilder.setCharAt(insertStatementHeaderBuilder.length() - 1, ')');
        insertStatementHeaderBuilder.append(" VALUES ");
        
        Iterator<DocPartRow> docPartRowIterator = docPartData.iterator();
        while (docPartRowIterator.hasNext()) {
            DocPartRow docPartRow = docPartRowIterator.next();
            docCounter++;
            if (insertStatementBuilder.length() == 0) {
                insertStatementBuilder.append(insertStatementHeaderBuilder);
            }
            insertStatementBuilder.append("(");
            for (KVValue<?> value : docPartRow) {
                insertStatementBuilder.append(getSqlValue(value))
                    .append(",");
            }
            insertStatementBuilder.setCharAt(insertStatementBuilder.length() - 1, ')');
            insertStatementBuilder.append(",");
            if (docCounter % maxBatchSize == 0 || !docPartRowIterator.hasNext()) {
                dsl.execute(insertStatementBuilder.substring(0, insertStatementBuilder.length() - 1));
                if (docPartRowIterator.hasNext()) {
                    insertStatementBuilder.delete(0, insertStatementBuilder.length());
                }
            }
        }
    }
    
    private String getSqlValue(KVValue<?> value) {
        return DSL.value(value, valueToJooqDataTypeProvider.getDataType(FieldType.from(value.getType()))).toString();
    }
    
    private void copyInsertPathDocuments(
            PGConnection connection,
            String schemaName,
            DocPartData docPartData) throws RetryTransactionException, SQLException, IOException {

        final int maxBatchSize = 1000;
        final StringBuilder sb = new StringBuilder(2048);
        final StringBuilder copyStatementBuilder = new StringBuilder();
        final CopyManager copyManager = connection.getCopyAPI();
        final MetaDocPart metaDocPart = docPartData.getMetaDocPart();
        copyStatementBuilder.append("COPY \"")
            .append(schemaName).append("\".\"").append(metaDocPart.getIdentifier())
            .append("(");
        
        Iterator<MetaField> metaFieldIterator = docPartData.orderedMetaFieldIterator();
        while (metaFieldIterator.hasNext()) {
            MetaField metaField = metaFieldIterator.next();
            copyStatementBuilder.append("\"")
                .append(metaField.getIdentifier())
                .append("\",")
                .append(",");
        }
        copyStatementBuilder.setCharAt(copyStatementBuilder.length() - 1, ')');
        copyStatementBuilder.append(" FROM STDIN");
        final String copyStatement = copyStatementBuilder.toString();
        
        Iterator<DocPartRow> docPartRowIterator = docPartData.iterator();
        int docCounter = 0;
        while (docPartRowIterator.hasNext()) {
            DocPartRow tableRow = docPartRowIterator.next();
            docCounter++;

            addToCopy(sb, tableRow);
            assert sb.length() != 0;

            if (docCounter % maxBatchSize == 0 || !docPartRowIterator.hasNext()) {
                executeCopy(copyManager, copyStatement, sb);
                assert sb.length() == 0;
            }
        }
        if (sb.length() > 0) {
            assert docCounter % maxBatchSize != 0;
            executeCopy(copyManager, copyStatement, sb);
            
            if (docPartRowIterator.hasNext()) {
                sb.delete(0, sb.length());
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void addToCopy(
            StringBuilder sb,
            DocPartRow docPartRow) {
        for (KVValue<?> value : docPartRow) {
            value.accept(DerbyValueToCopyConverter.INSTANCE, sb);
            sb.append('\t');
        }
        sb.setCharAt(sb.length() - 1, '\n');
    }

    protected Integer translateSubDocIndexToDatabase(int index) {
        if (index == 0) {
            return null;
        }
        return index;
    }

    private void executeCopy(CopyManager copyManager, String copyStatement, final StringBuilder sb) throws SQLException, IOException {
        Reader reader = new StringBuilderReader(sb);
        
        copyManager.copyIn(copyStatement, reader);
    }
    
    public interface PGConnection extends Connection {
        public CopyManager getCopyAPI();
    }
    
    public interface CopyManager {
        public void copyIn(String copyStatement, Reader reader);
    }

    private static class StringBuilderReader extends Reader {

        private final StringBuilder sb;
        private int readerIndex = 0;

        public StringBuilderReader(StringBuilder sb) {
            this.sb = sb;
        }

        @Override
        public int read(char[] cbuf, int off, int len) throws IOException {
            if (readerIndex == sb.length()) {
                return -1;
            }
            int newReaderIndex = Math.min(sb.length(), readerIndex + len);
            sb.getChars(readerIndex, newReaderIndex, cbuf, off);
            int diff = newReaderIndex - readerIndex;
            readerIndex = newReaderIndex;
            return diff;
        }

        @Override
        public void close() {
        }

    }

    @Override
    public int reserveRids(DSLContext dsl, String database, String collection, TableRef tableRef, int count) {
        throw new ToroImplementationException("Not implemented yet");
    }

    @Override
    public DataTypeForKV<?> getDataType(FieldType type) {
        return valueToJooqDataTypeProvider.getDataType(type);
    }

    @Override
    public void handleRetryException(Context context, SQLException sqlException) throws RetryTransactionException {
        if (context == Context.batchUpdate && "40001".equals(sqlException.getSQLState())) {
            throw new RetryTransactionException(sqlException);
        }
    }

    @Override
    public void handleRetryException(Context context, DataAccessException dataAccessException) throws RetryTransactionException {
        if (context == Context.batchUpdate && "40001".equals(dataAccessException.sqlState())) {
            throw new RetryTransactionException(dataAccessException);
        }
    }

    @Override
    public boolean isSameIdentifier(@Nonnull String leftIdentifier, @Nonnull String rightIdentifier) {
        return leftIdentifier.equals(rightIdentifier); //leftIdentifier.toLowerCase(Locale.US).equals(rightIdentifier.toLowerCase(Locale.US));
    }
}
