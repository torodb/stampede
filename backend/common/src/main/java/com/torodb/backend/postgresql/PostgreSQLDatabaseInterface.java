package com.torodb.backend.postgresql;

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
import com.torodb.backend.InternalField;
import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.backend.converters.jooq.ValueToJooqDataTypeProvider;
import com.torodb.backend.index.NamedDbIndex;
import com.torodb.backend.meta.TorodbSchema;
import com.torodb.backend.postgresql.converters.PostgreSQLValueToCopyConverter;
import com.torodb.backend.postgresql.converters.jooq.PostgreSQLValueToJooqDataTypeProvider;
import com.torodb.backend.postgresql.tables.PostgreSQLMetaCollectionTable;
import com.torodb.backend.postgresql.tables.PostgreSQLMetaDatabaseTable;
import com.torodb.backend.postgresql.tables.PostgreSQLMetaDocPartTable;
import com.torodb.backend.postgresql.tables.PostgreSQLMetaFieldTable;
import com.torodb.backend.tables.MetaCollectionTable;
import com.torodb.backend.tables.MetaDatabaseTable;
import com.torodb.backend.tables.MetaDocPartTable;
import com.torodb.backend.tables.MetaDocPartTable.DocPartTableFields;
import com.torodb.backend.tables.MetaFieldTable;
import com.torodb.core.TableRef;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.d2r.DocPartResults;
import com.torodb.core.d2r.DocPartRow;
import com.torodb.core.exceptions.SystemException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.kvdocument.values.KVValue;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 *
 */
@Singleton
public class PostgreSQLDatabaseInterface implements DatabaseInterface {
    private static final Logger LOGGER
        = LogManager.getLogger(PostgreSQLDatabaseInterface.class);

    private static final long serialVersionUID = 784618502;

    private static final String[] RESTRICTED_SCHEMA_NAMES = new String[] {
            TorodbSchema.TORODB_SCHEMA,
            "pg_catalog",
            "information_schema",
    };
    {
        Arrays.sort(RESTRICTED_SCHEMA_NAMES);
    }

    private static final String[] RESTRICTED_COLUMN_NAMES = new String[] {
            DocPartTableFields.DID.fieldName,
            DocPartTableFields.RID.fieldName,
            DocPartTableFields.PID.fieldName,
            DocPartTableFields.SEQ.fieldName,
            "oid",
            "tableoid",
            "xmin",
            "xmax",
            "cmin",
            "cmax",
            "ctid",
    };
    {
        Arrays.sort(RESTRICTED_COLUMN_NAMES);
    }

    private final ValueToJooqDataTypeProvider valueToJooqDataTypeProvider;
    private final FieldComparator fieldComparator = new FieldComparator();
    private final PostgreSQLMetaDatabaseTable metaDatabaseTable = new PostgreSQLMetaDatabaseTable();
    private final PostgreSQLMetaCollectionTable metaCollectionTable = new PostgreSQLMetaCollectionTable();
    private final PostgreSQLMetaDocPartTable metaDocPartTable = new PostgreSQLMetaDocPartTable();
    private final PostgreSQLMetaFieldTable metaFieldTable = new PostgreSQLMetaFieldTable();

    @Inject
    public PostgreSQLDatabaseInterface() {
        this.valueToJooqDataTypeProvider = PostgreSQLValueToJooqDataTypeProvider.getInstance();
    }

    private void readObject(java.io.ObjectInputStream stream)
            throws IOException, ClassNotFoundException {
        //TODO: Try to remove make DatabaseInterface not serializable
        stream.defaultReadObject();
    }

    @Nonnull
    @Override
    public PostgreSQLMetaDatabaseTable getMetaDatabaseTable() {
        return metaDatabaseTable;
    }

    @Nonnull
    @Override
    public PostgreSQLMetaCollectionTable getMetaCollectionTable() {
        return metaCollectionTable;
    }

    @Nonnull
    @Override
    public PostgreSQLMetaDocPartTable getMetaDocPartTable() {
        return metaDocPartTable;
    }

    @Nonnull
    @Override
    public PostgreSQLMetaFieldTable getMetaFieldTable() {
        return metaFieldTable;
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
            if (o1.getName().equals(DocPartTableFields.DID)) {
                return -1;
            } else if (o2.getName().equals(DocPartTableFields.DID)) {
                return 1;
            }
            if (o1.getName().equals(DocPartTableFields.RID)) {
                return -1;
            } else if (o2.getName().equals(DocPartTableFields.RID)) {
                return 1;
            }
            if (o1.getName().equals(DocPartTableFields.PID)) {
                return -1;
            } else if (o2.getName().equals(DocPartTableFields.PID)) {
                return 1;
            }
            if (o1.getName().equals(DocPartTableFields.SEQ)) {
                return -1;
            } else if (o2.getName().equals(DocPartTableFields.SEQ)) {
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
    public void createMetaDatabaseTable(DSLContext dsl) {
    	String schemaName = metaDatabaseTable.getSchema().getName();
    	String tableName = metaDatabaseTable.getName();
    	
        String statement = new StringBuilder()
        		 .append("CREATE TABLE ")
                 .append(fullTableName(schemaName, tableName))
                 .append(" (")
                 .append(MetaDatabaseTable.TableFields.NAME.name()).append("             varchar     PRIMARY KEY     ,")
                 .append(MetaDatabaseTable.TableFields.IDENTIFIER.name()).append("       varchar     NOT NULL UNIQUE ")
                 .append(")")
                .toString();
        executeStatement(dsl, statement, Context.ddl);
    }
    
    @Override
    public void createMetaCollectionTable(DSLContext dsl) {
    	String schemaName = metaCollectionTable.getSchema().getName();
    	String tableName = metaCollectionTable.getName();
    	String statement = new StringBuilder()
    			 .append("CREATE TABLE ")
                 .append(fullTableName(schemaName, tableName))
                 .append(" (")
                 .append(MetaCollectionTable.TableFields.DATABASE.name()).append("         varchar     NOT NULL        ,")
                 .append(MetaCollectionTable.TableFields.NAME.name()).append("             varchar     NOT NULL        ,")
                 .append("    PRIMARY KEY (").append(MetaCollectionTable.TableFields.DATABASE.name()).append(",")
                     .append(MetaCollectionTable.TableFields.NAME.name()).append("),")
                 .append(")")
                 .toString();
    	executeStatement(dsl, statement, Context.ddl);
    }

    @Override
    public void createMetaDocPartTable(DSLContext dsl) {
    	String schemaName = metaDocPartTable.getSchema().getName();
    	String tableName = metaDocPartTable.getName();
    	String statement = new StringBuilder()
    			.append("CREATE TABLE ")
                .append(fullTableName(schemaName, tableName))
                .append(" (")
                .append(MetaDocPartTable.TableFields.DATABASE.name()).append("         varchar     NOT NULL        ,")
                .append(MetaDocPartTable.TableFields.COLLECTION.name()).append("       varchar     NOT NULL        ,")
                .append(MetaDocPartTable.TableFields.TABLE_REF.name()).append("        varchar[]   NOT NULL        ,")
                .append(MetaDocPartTable.TableFields.IDENTIFIER.name()).append("       varchar     NOT NULL        ,")
                .append(MetaDocPartTable.TableFields.LAST_RID.name()).append("         int         NOT NULL        ,")
                .append("    PRIMARY KEY (").append(MetaDocPartTable.TableFields.DATABASE.name()).append(",")
                    .append(MetaDocPartTable.TableFields.COLLECTION.name()).append(",")
                    .append(MetaDocPartTable.TableFields.TABLE_REF.name()).append("),")
                .append("    UNIQUE KEY (").append(MetaDocPartTable.TableFields.DATABASE.name()).append(",")
                    .append(MetaDocPartTable.TableFields.IDENTIFIER.name()).append(")")
                .append(")")
                .toString();
    	executeStatement(dsl, statement, Context.ddl);    	
    }

    @Override
    public @Nonnull String createMetaFieldTableStatement(@Nonnull String schemaName, @Nonnull String tableName) {
        return new StringBuilder()
                .append("CREATE TABLE ")
                .append(fullTableName(schemaName, tableName))
                .append(" (")
                .append(MetaFieldTable.TableFields.DATABASE.name()).append("         varchar     NOT NULL        ,")
                .append(MetaFieldTable.TableFields.COLLECTION.name()).append("       varchar     NOT NULL        ,")
                .append(MetaFieldTable.TableFields.TABLE_REF.name()).append("        varchar[]   NOT NULL        ,")
                .append(MetaFieldTable.TableFields.NAME.name()).append("             varchar     NOT NULL        ,")
                .append(MetaFieldTable.TableFields.IDENTIFIER.name()).append("       varchar     NOT NULL        ,")
                .append(MetaFieldTable.TableFields.TYPE.name()).append("             varchar     NOT NULL        ,")
                .append("    PRIMARY KEY (").append(MetaFieldTable.TableFields.DATABASE.name()).append(",")
                .append(MetaFieldTable.TableFields.COLLECTION.name()).append(",")
                .append(MetaFieldTable.TableFields.TABLE_REF.name()).append(",")
                    .append(MetaFieldTable.TableFields.NAME.name()).append("),")
                .append("    UNIQUE KEY (").append(MetaFieldTable.TableFields.DATABASE.name()).append(",")
                    .append(MetaFieldTable.TableFields.COLLECTION.name()).append(",")
                    .append(MetaFieldTable.TableFields.TABLE_REF.name()).append(",")
                    .append(MetaFieldTable.TableFields.IDENTIFIER.name()).append(")")
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
    public void dropSchema(@Nonnull DSLContext dsl, @Nonnull String schemaName) {
        Connection c = dsl.configuration().connectionProvider().acquire();
        String query = "DROP SCHEMA \"" + schemaName + "\" CASCADE";
        try (PreparedStatement ps = c.prepareStatement(query)) {
            ps.executeUpdate();
        } catch (SQLException e) {
        	handleRetryException(Context.ddl, e);
        	throw new SystemException(e);
		} finally {
            dsl.configuration().connectionProvider().release(c);
        }
    }

    @Nonnull
    @Override
    public DocPartResults<ResultSet> getCollectionResultSets(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase, @Nonnull MetaCollection metaCollection, @Nonnull Collection<Integer> requestedDocs) {
        throw new UnsupportedOperationException("Not implemented yet");
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

    @Override
    public void createSchema(@Nonnull DSLContext dsl, @Nonnull String schemaName){
        Connection c = dsl.configuration().connectionProvider().acquire();
        String query = "CREATE SCHEMA IF NOT EXISTS \"" + schemaName + "\"";
        try (PreparedStatement ps = c.prepareStatement(query)) {
            ps.executeUpdate();
        } catch (SQLException e) {
        	handleRetryException(Context.ddl, e);
        	throw new SystemException(e);
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
    public void insertDocPartData(DSLContext dsl, String schemaName, DocPartData docPartData) {
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
                    LOGGER.warn("It was impossible to use the PostgreSQL way to "
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
            throw new SystemException(ex);
        } catch (SQLException ex) {
            handleRetryException(Context.insert, ex);
            throw new SystemException(ex);
        } catch (IOException ex) {
            throw new SystemException(ex);
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
            DocPartData docPartData) throws SQLException, IOException {

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

    private void addToCopy(
            StringBuilder sb,
            DocPartRow docPartRow) {
        for (KVValue<?> value : docPartRow) {
            value.accept(PostgreSQLValueToCopyConverter.INSTANCE, sb);
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
    public int consumeRids(DSLContext dsl, String database, String collection, TableRef tableRef, int count) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public DataTypeForKV<?> getDataType(FieldType type) {
        return valueToJooqDataTypeProvider.getDataType(type);
    }

    private static final String[] ROLLBACK_EXCEPTIONS = new String[] { "40001", "40P01" };
    {
        Arrays.sort(ROLLBACK_EXCEPTIONS);
    }
    
    @Override
    public void handleRetryException(Context context, SQLException sqlException) {
        if (Arrays.binarySearch(ROLLBACK_EXCEPTIONS, sqlException.getSQLState()) >= 0) {
            throw new RollbackException(sqlException);
        }
    }

    @Override
    public void handleRetryException(Context context, DataAccessException dataAccessException) {
        if (Arrays.binarySearch(ROLLBACK_EXCEPTIONS, dataAccessException.sqlState()) >= 0) {
            throw new RollbackException(dataAccessException);
        }
    }

    @Override
    public boolean isSameIdentifier(String leftIdentifier, String rightIdentifier) {
        return leftIdentifier.equals(rightIdentifier);
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
    public Collection<InternalField<?>> getDocPartTableInternalFields(MetaDocPart metaDocPart) {
        TableRef tableRef = metaDocPart.getTableRef();
        if (tableRef.isRoot()) {
            return metaDocPartTable.ROOT_FIELDS;
        } else if (tableRef.getParent().get().isRoot()) {
            return metaDocPartTable.FIRST_FIELDS;
        }
        
        return metaDocPartTable.FIELDS;
    }
    
	@Override
	public Integer getLastRowIUsed(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase, @Nonnull MetaCollection metaCollection, @Nonnull MetaDocPart metaDocPart) throws SQLException {
		Connection connection = dsl.configuration().connectionProvider().acquire();
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT max(\"")
            .append(DocPartTableFields.RID.fieldName)
            .append("\" FROM \"")
            .append(metaDatabase.getIdentifier())
            .append("\".\"")
            .append(metaDocPart.getIdentifier());
            
            try (PreparedStatement preparedStatement = connection.prepareStatement(sb.toString())){
            	ResultSet rs = preparedStatement.executeQuery();
            	rs.next();
            	int maxId = rs.getInt(1);
            	if (rs.wasNull()){
            		return -1;
            	}
            	return maxId;
            }
        } finally {
            dsl.configuration().connectionProvider().release(connection);
        }
	}
	
	@Override
	public void addMetaDatabase(DSLContext dsl, String databaseName, String databaseIdentifier) {
        dsl.insertInto(metaDatabaseTable)
            .set(metaDatabaseTable.newRecord().values(databaseName, databaseIdentifier))
            .execute();		
	}

	@Override
	public void addMetaCollection(DSLContext dsl, String databaseName, String collectionName, String collectionIdentifier) {
        dsl.insertInto(metaCollectionTable)
            .set(metaCollectionTable.newRecord()
            .values(databaseName, collectionName, collectionIdentifier))
            .execute();		
	}
	
	@Override
	public void addMetaDocPart(DSLContext dsl, String databaseName, String collectionName, TableRef tableRef,
			String docPartIdentifier) {
        dsl.insertInto(metaDocPartTable)
            .set(metaDocPartTable.newRecord()
            .values(databaseName, collectionName, tableRef, docPartIdentifier))
            .execute();		
	}
	
    @Override
	public void createDocPartTable(DSLContext dsl, String schemaName, String tableName, Collection<Field<?>> fields) {
		StringBuilder sb = new StringBuilder();
		sb.append("CREATE TABLE ")
		  .append(fullTableName(schemaName, tableName))
		  .append(" (");
		Configuration conf = dsl.configuration();
		int cont = 0;
		for (Field<?> field : getFieldIterator(fields)) {
			if (cont > 0) {
				sb.append(',');
			}
			sb.append('"').append(field.getName()).append("\" ").append(field.getDataType().getCastTypeName(conf));
			cont++;
		}
		sb.append(')');
		dsl.execute(sb.toString());
	}

	@Override
	public void addMetaField(DSLContext dsl, String databaseName, String collectionName, TableRef tableRef,
			String fieldName, String fieldIdentifier, FieldType type) {
        dsl.insertInto(metaFieldTable)
            .set(metaFieldTable.newRecord()
            	.values(databaseName, collectionName, tableRef, fieldName, fieldIdentifier, type))
            .execute();
	}
	
	@Override
    public void addColumnToDocPartTable(DSLContext dsl, String schemaName, String tableName, Field<?> field) {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ")
	        .append(fullTableName(schemaName, tableName))
	        .append(" ADD COLUMN \"")
	        .append(field.getName())
	        .append("\" ")
            .append(field.getDataType().getCastTypeName(dsl.configuration()));
        dsl.execute(sb.toString());
    }
	
    private void executeStatement(DSLContext dsl, String statement, Context context){
    	Connection c = dsl.configuration().connectionProvider().acquire();
        try (PreparedStatement ps = c.prepareStatement(statement)) {
            ps.execute();
        } catch (SQLException ex) {
        	handleRetryException(context, ex);
            throw new SystemException(ex);
		} finally {
            dsl.configuration().connectionProvider().release(c);
        }    	
    }
}
