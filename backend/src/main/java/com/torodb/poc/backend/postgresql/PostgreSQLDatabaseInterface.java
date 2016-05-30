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


package com.torodb.poc.backend.postgresql;

import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.sql.Array;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.jooq.Configuration;
import org.jooq.ConnectionProvider;
import org.jooq.DSLContext;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.InsertValuesStep1;
import org.jooq.Record;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.torodb.kvdocument.types.KVType;
import com.torodb.kvdocument.values.KVMongoObjectId;
import com.torodb.kvdocument.values.KVMongoTimestamp;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.poc.backend.DatabaseInterface;
import com.torodb.poc.backend.TableToDDL.PathSnapshot;
import com.torodb.poc.backend.TableToDDL.TableColumn;
import com.torodb.poc.backend.TableToDDL.TableData;
import com.torodb.poc.backend.TableToDDL.TableRow;
import com.torodb.poc.backend.converters.jooq.ValueToJooqConverterProvider;
import com.torodb.poc.backend.converters.jooq.ValueToJooqDataTypeProvider;
import com.torodb.poc.backend.mocks.KVTypeToSqlType;
import com.torodb.poc.backend.mocks.Path;
import com.torodb.poc.backend.mocks.RetryTransactionException;
import com.torodb.poc.backend.mocks.SplitDocument;
import com.torodb.poc.backend.mocks.ToroImplementationException;
import com.torodb.poc.backend.mocks.ToroRuntimeException;
import com.torodb.poc.backend.postgresql.converters.PostgreSQLKVTypeToSqlType;
import com.torodb.poc.backend.postgresql.converters.PostgreSQLValueToCopyConverter;
import com.torodb.poc.backend.postgresql.converters.jooq.PostgreSQLValueToJooqConverterProvider;
import com.torodb.poc.backend.postgresql.converters.jooq.PostgreSQLValueToJooqDataTypeProvider;
import com.torodb.poc.backend.postgresql.tables.PostgreSQLCollectionTable;
import com.torodb.poc.backend.postgresql.tables.PostgreSQLContainerTable;
import com.torodb.poc.backend.postgresql.tables.PostgreSQLDatabaseTable;
import com.torodb.poc.backend.postgresql.tables.PostgreSQLFieldTable;
import com.torodb.poc.backend.sql.index.NamedDbIndex;
import com.torodb.poc.backend.tables.CollectionTable;
import com.torodb.poc.backend.tables.ContainerTable;
import com.torodb.poc.backend.tables.DatabaseTable;
import com.torodb.poc.backend.tables.FieldTable;
import com.torodb.poc.backend.tables.PathDocTable;
import com.torodb.poc.backend.tables.records.CollectionRecord;
import com.torodb.poc.backend.tables.records.ContainerRecord;
import com.torodb.poc.backend.tables.records.DatabaseRecord;
import com.torodb.poc.backend.tables.records.FieldRecord;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 *
 */
@Singleton
public class PostgreSQLDatabaseInterface implements DatabaseInterface {
    private static final org.slf4j.Logger LOGGER
        = LoggerFactory.getLogger(PostgreSQLDatabaseInterface.class);

    private static final long serialVersionUID = 484638503;

    private final ValueToJooqConverterProvider valueToJooqConverterProvider;
    private final ValueToJooqDataTypeProvider valueToJooqDataTypeProvider;
    private final KVTypeToSqlType kVTypeToSqlType;
    private final FieldComparator fieldComparator = new FieldComparator();

    @Inject
    public PostgreSQLDatabaseInterface(KVTypeToSqlType kVTypeToSqlType) {
        this.valueToJooqConverterProvider = PostgreSQLValueToJooqConverterProvider.getInstance();
        this.valueToJooqDataTypeProvider = PostgreSQLValueToJooqDataTypeProvider.getInstance();
        this.kVTypeToSqlType = kVTypeToSqlType;
    }

    private void readObject(java.io.ObjectInputStream stream)
            throws IOException, ClassNotFoundException {
        //TODO: Try to remove make DatabaseInterface not serializable
        stream.defaultReadObject();
    }

    @Nonnull
    @Override
    public DatabaseTable<?> getDatabaseTable() {
        return new PostgreSQLDatabaseTable();
    }

    @Nonnull
    @Override
    public CollectionTable<?> getCollectionTable() {
        return new PostgreSQLCollectionTable();
    }

    @Nonnull
    @Override
    public ContainerTable<?> getContainerTable() {
        return new PostgreSQLContainerTable();
    }

    @Nonnull
    @Override
    public FieldTable<?> getFieldTable() {
        return new PostgreSQLFieldTable();
    }

    @Override
    public @Nonnull ValueToJooqConverterProvider getValueToJooqConverterProvider() {
        return valueToJooqConverterProvider;
    }

    @Override
    public @Nonnull ValueToJooqDataTypeProvider getValueToJooqDataTypeProvider() {
        return valueToJooqDataTypeProvider;
    }

    @Override
    public String createIndexStatement(PathDocTable table, Field<?> field, Configuration conf) {
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
    public String createPathDocTableStatement(String schemaName, String tableName, List<Field<?>> fields, Configuration conf) {
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
    public String addColumnsToTableStatement(String schemaName, String tableName, List<Field<?>> fields, Configuration conf) {
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
        if (field.getConverter() != null) {
            Class<?> fieldType = field.getDataType().getType();
            if (fieldType.equals(KVMongoObjectId.class)) {
                return PostgreSQLKVTypeToSqlType.MONGO_OBJECT_ID_TYPE;
            }
            if (fieldType.equals(KVMongoTimestamp.class)) {
                return PostgreSQLKVTypeToSqlType.MONGO_TIMESTAMP_TYPE;
            }
        }
        return field.getDataType().getTypeName(conf);
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
            if (o1.getName().equals(PathDocTable.DID_COLUMN_NAME)) {
                return -1;
            } else if (o2.getName().equals(PathDocTable.DID_COLUMN_NAME)) {
                return 1;
            }
            if (o1.getName().equals(PathDocTable.RID_COLUMN_NAME)) {
                return -1;
            } else if (o2.getName().equals(PathDocTable.RID_COLUMN_NAME)) {
                return 1;
            }
            if (o1.getName().equals(PathDocTable.PID_COLUMN_NAME)) {
                return -1;
            } else if (o2.getName().equals(PathDocTable.PID_COLUMN_NAME)) {
                return 1;
            }
            if (o1.getName().equals(PathDocTable.SEQ_COLUMN_NAME)) {
                return -1;
            } else if (o2.getName().equals(PathDocTable.SEQ_COLUMN_NAME)) {
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
        if (str.length() > 63) {
            throw new IllegalArgumentException(str + " is too long to be a "
                    + "valid PostgreSQL name. By default names must be shorter "
                    + "than 64, but it has " + str.length() + " characters");
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
    public @Nonnull String createDatabaseTableStatement(@Nonnull String schemaName, @Nonnull String tableName) {
        return new StringBuilder()
                .append("CREATE TABLE ")
                .append(fullTableName(schemaName, tableName))
                .append(" (")
                .append(DatabaseTable.TableFields.NAME.name()).append("             varchar     PRIMARY KEY     ,")
                .append(DatabaseTable.TableFields.SCHEMA_NAME.name()).append("      varchar     NOT NULL UNIQUE ")
                .append(")")
                .toString();
    }

    @Override
    public @Nonnull String createCollectionTableStatement(@Nonnull String schemaName, @Nonnull String tableName) {
        return new StringBuilder()
                .append("CREATE TABLE ")
                .append(fullTableName(schemaName, tableName))
                .append(" (")
                .append(CollectionTable.TableFields.DATABASE.name()).append("         varchar     NOT NULL        ,")
                .append(CollectionTable.TableFields.NAME.name()).append("             varchar     NOT NULL        ,")
                .append(CollectionTable.TableFields.TABLE_NAME.name()).append("       varchar     NOT NULL        ,")
                .append(CollectionTable.TableFields.CAPPED.name()).append("           boolean     NOT NULL        ,")
                .append(CollectionTable.TableFields.MAX_SIZE.name()).append("         int         NOT NULL        ,")
                .append(CollectionTable.TableFields.MAX_ELEMENTS.name()).append("     int         NOT NULL        ,")
                .append(CollectionTable.TableFields.OTHER.name()).append("            jsonb                       ,")
                .append(CollectionTable.TableFields.STORAGE_ENGINE.name()).append("   varchar     NOT NULL        ")
                .append(CollectionTable.TableFields.LAST_DID.name()).append("         int         NOT NULL        ,")
                .append("    PRIMARY KEY (").append(CollectionTable.TableFields.DATABASE.name()).append(",")
                    .append(CollectionTable.TableFields.NAME.name()).append("),")
                .append("    UNIQUE KEY (").append(CollectionTable.TableFields.DATABASE.name()).append(",")
                    .append(CollectionTable.TableFields.TABLE_NAME.name()).append(")")
                .append(")")
                .toString();
    }

    @Override
    public @Nonnull String createContainerTableStatement(@Nonnull String schemaName, @Nonnull String tableName) {
        return new StringBuilder()
                .append("CREATE TABLE ")
                .append(fullTableName(schemaName, tableName))
                .append(" (")
                .append(ContainerTable.TableFields.DATABASE.name()).append("         varchar     NOT NULL        ,")
                .append(ContainerTable.TableFields.COLLECTION.name()).append("       varchar     NOT NULL        ,")
                .append(ContainerTable.TableFields.PATH.name()).append("             varchar     NOT NULL        ,")
                .append(ContainerTable.TableFields.TABLE_NAME.name()).append("       varchar     NOT NULL        ,")
                .append(ContainerTable.TableFields.LAST_RID.name()).append("         int         NOT NULL        ,")
                .append("    PRIMARY KEY (").append(ContainerTable.TableFields.DATABASE.name()).append(",")
                    .append(ContainerTable.TableFields.COLLECTION.name()).append(",")
                    .append(ContainerTable.TableFields.PATH.name()).append("),")
                .append("    UNIQUE KEY (").append(ContainerTable.TableFields.DATABASE.name()).append(",")
                    .append(ContainerTable.TableFields.TABLE_NAME.name()).append(")")
                .append(")")
                .toString();
    }

    @Override
    public @Nonnull String createFieldTableStatement(@Nonnull String schemaName, @Nonnull String tableName) {
        return new StringBuilder()
                .append("CREATE TABLE ")
                .append(fullTableName(schemaName, tableName))
                .append(" (")
                .append(FieldTable.TableFields.DATABASE.name()).append("         varchar     NOT NULL        ,")
                .append(FieldTable.TableFields.COLLECTION.name()).append("       varchar     NOT NULL        ,")
                .append(FieldTable.TableFields.PATH.name()).append("             varchar     NOT NULL        ,")
                .append(FieldTable.TableFields.NAME.name()).append("             varchar     NOT NULL        ,")
                .append(FieldTable.TableFields.COLUMN_NAME.name()).append("      varchar     NOT NULL        ,")
                .append(FieldTable.TableFields.COLUMN_TYPE.name()).append("      varchar     NOT NULL        ,")
                .append("    PRIMARY KEY (").append(FieldTable.TableFields.DATABASE.name()).append(",")
                .append(FieldTable.TableFields.COLLECTION.name()).append(",")
                .append(FieldTable.TableFields.PATH.name()).append(",")
                    .append(FieldTable.TableFields.NAME.name()).append("),")
                .append("    UNIQUE KEY (").append(FieldTable.TableFields.DATABASE.name()).append(",")
                    .append(FieldTable.TableFields.COLLECTION.name()).append(",")
                    .append(FieldTable.TableFields.PATH.name()).append(",")
                    .append(FieldTable.TableFields.COLUMN_NAME.name()).append(")")
                .append(")")
                .toString();
    }

    @Override
    public @Nonnull String createIndexesTableStatement(
            @Nonnull String schemaName, @Nonnull String tableName, @Nonnull String indexNameColumn, @Nonnull String indexOptionsColumn
    ) {
        return new StringBuilder()
                .append("CREATE TABLE ")
                .append(fullTableName(schemaName, tableName))
                .append(" (")
                .append(indexNameColumn).append("       varchar     PRIMARY KEY,")
                .append(indexOptionsColumn).append("    jsonb       NOT NULL")
                .append(")")
                .toString();
    }

    private static @Nonnull StringBuilder fullTableName(@Nonnull String schemaName, @Nonnull String tableName) {
        return new StringBuilder()
                .append("\"").append(schemaName).append("\"")
                .append(".")
                .append("\"").append(tableName).append("\"");
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
    public void insertPathDocuments(DSLContext dsl, String schemaName, PathSnapshot pathSnapshot, TableData tableData) throws RetryTransactionException {
        Preconditions.checkArgument(tableData.size() != 0, "Called insert with 0 documents");
        Preconditions.checkArgument(tableData.iterator().next().size() != 0, "Called insert with 0 documents");
        
        Connection connection = dsl.configuration().connectionProvider().acquire();
        try {
            int maxCappedSize = 10;
            int cappedSize = Iterables.size(
                    Iterables.limit(tableData, maxCappedSize)
            );

            if (cappedSize < maxCappedSize) { //there are not enough elements on the insert => fallback
                LOGGER.debug(
                        "The insert window is not big enough to use copy (the "
                                + "limit is {}, the real size is {}).",
                        maxCappedSize,
                        cappedSize
                );
                standardInsertPathDocuments(dsl, schemaName, pathSnapshot, tableData);
            }
            else {
                if (!connection.isWrapperFor(PGConnection.class)) {
                    LOGGER.warn("It was impossible to use the PostgreSQL way to "
                            + "insert documents. Inserting using the standard "
                            + "implementation");
                    standardInsertPathDocuments(dsl, schemaName, pathSnapshot, tableData);
                }
                else {
                    copyInsertPathDocuments(
                            connection.unwrap(PGConnection.class),
                            schemaName,
                            pathSnapshot,
                            tableData
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

    protected void standardInsertPathDocuments(DSLContext dsl, String schemaName, PathSnapshot pathSnapshot, TableData tableData) {
        final int maxBatchSize = 1000;
        final StringBuilder sb = new StringBuilder(2048);
        int docCounter = 0;
        for (TableRow tableRow : tableData) {
            docCounter++;
            if (sb.length() == 0) {
                sb.append("INSERT INTO \"")
                    .append(schemaName)
                    .append("\".\"")
                    .append(pathSnapshot.getTableName())
                    .append("\" (");
                
                StringBuilder values = new StringBuilder();
                for (TableColumn tableColumn : tableRow) {
                    sb.append("\"")
                        .append(pathSnapshot.getColumnName(tableColumn.getName()))
                        .append("\",");
                    values.append(getSqlValue(tableColumn))
                        .append(",");
                }
                sb.setCharAt(sb.length() - 1, ')');
                values.setCharAt(values.length() - 1, ')');
                sb.append(" VALUES (")
                    .append(values)
                    .append(",");
            }
            else {
                sb.append("(");
                for (TableColumn tableColumn : tableRow) {
                    sb.append(getSqlValue(tableColumn))
                        .append(",");
                }
                sb.setCharAt(sb.length() - 1, ')');
                sb.append(",");
            }
            if (docCounter % maxBatchSize == 0) {
                dsl.execute(sb.substring(0, sb.length() - 2));
                sb.delete(0, sb.length());
                assert sb.length() == 0;
            }
        }
    }
    
    private String getSqlValue(TableColumn tableColumn) {
        final KVValue<? extends Serializable> value = tableColumn.getValue();
        return DSL.value(value, valueToJooqDataTypeProvider.getDataType(value.getType())).toString();
    }
    
    private void copyInsertPathDocuments(
            PGConnection connection,
            String schemaName,
            PathSnapshot pathSnapshot,
            TableData tableData) throws RetryTransactionException, SQLException, IOException {

        final int maxBatchSize = 1000;
        final StringBuilder sb = new StringBuilder(2048);
        final StringBuilder copyStamentBuilder = new StringBuilder();
        final CopyManager copyManager = connection.getCopyAPI();
        copyStamentBuilder.append("COPY \"")
            .append(schemaName).append("\".\"").append(pathSnapshot.getTableName())
            .append("(");
        TableRow firstTableRow = tableData.iterator().next();
        copyStamentBuilder.append(") FROM STDIN ");
        final String copyStatement = copyStamentBuilder.toString();
        
        int docCounter = 0;
        for (TableRow tableRow : tableData) {
            docCounter++;

            addToCopy(sb, tableRow);
            assert sb.length() != 0;

            if (docCounter % maxBatchSize == 0) {
                executeCopy(copyManager, copyStatement, sb);
                assert sb.length() == 0;
            }
        }
        if (sb.length() > 0) {
            assert docCounter % maxBatchSize != 0;
            executeCopy(copyManager, copyStatement, sb);
        }
    }

    @SuppressWarnings("unchecked")
    private void addToCopy(
            StringBuilder sb,
            TableRow tableRow) {
        for (TableColumn tableColumn : tableRow) {
            tableColumn.getValue().accept(PostgreSQLValueToCopyConverter.INSTANCE, sb);
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

        sb.delete(0, sb.length());
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
    public DataType<?> getDataType(String type) {
        throw new ToroImplementationException("Not implemented yet");
    }

    @Override
    public int reserveDids(DSLContext dsl, String database, String collection, int count) {
        throw new ToroImplementationException("Not implemented yet");
    }

    @Override
    public int reserveRids(DSLContext dsl, String database, String collection, Path path, int count) {
        throw new ToroImplementationException("Not implemented yet");
    }

    @Override
    public Iterable<DatabaseRecord> getDatabases(DSLContext dsl) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Iterable<CollectionRecord> getCollections(DSLContext dsl, String database) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Iterable<ContainerRecord> getContainers(DSLContext dsl, String database, String collection) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Iterable<FieldRecord> getFields(DSLContext dsl, String database, String collection, Path path) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DataType<?> getDataType(KVType type) {
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
}
