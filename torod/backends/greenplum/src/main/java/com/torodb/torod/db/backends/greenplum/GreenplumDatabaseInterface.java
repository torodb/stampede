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


package com.torodb.torod.db.backends.greenplum;

import java.io.IOException;
import java.sql.Array;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import org.jooq.DSLContext;

import com.torodb.torod.core.connection.exceptions.RetryTransactionException;
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.language.projection.Projection;
import com.torodb.torod.core.pojos.IndexedAttributes.IndexType;
import com.torodb.torod.core.subdocument.SimpleSubDocTypeBuilderProvider;
import com.torodb.torod.core.subdocument.SubDocType;
import com.torodb.torod.core.subdocument.SubDocType.Builder;
import com.torodb.torod.db.backends.ArraySerializer;
import com.torodb.torod.db.backends.DatabaseInterface;
import com.torodb.torod.db.backends.converters.ScalarTypeToSqlType;
import com.torodb.torod.db.backends.converters.StructureConverter;
import com.torodb.torod.db.backends.converters.array.ValueToArrayConverterProvider;
import com.torodb.torod.db.backends.converters.array.ValueToArrayDataTypeProvider;
import com.torodb.torod.db.backends.converters.jooq.ValueToJooqConverterProvider;
import com.torodb.torod.db.backends.converters.jooq.ValueToJooqDataTypeProvider;
import com.torodb.torod.db.backends.converters.json.ValueToJsonConverterProvider;
import com.torodb.torod.db.backends.exceptions.InvalidDatabaseException;
import com.torodb.torod.db.backends.greenplum.converters.array.GreenplumValueToArrayConverterProvider;
import com.torodb.torod.db.backends.greenplum.converters.array.GreenplumValueToArrayDataTypeProvider;
import com.torodb.torod.db.backends.greenplum.converters.jooq.GreenplumValueToJooqConverterProvider;
import com.torodb.torod.db.backends.greenplum.converters.jooq.GreenplumValueToJooqDataTypeProvider;
import com.torodb.torod.db.backends.greenplum.converters.json.GreenplumValueToJsonConverterProvider;
import com.torodb.torod.db.backends.greenplum.meta.GreenplumIndexStorage;
import com.torodb.torod.db.backends.greenplum.meta.GreenplumStructuresCache;
import com.torodb.torod.db.backends.greenplum.tables.GreenplumCollectionsTable;
import com.torodb.torod.db.backends.meta.CollectionSchema;
import com.torodb.torod.db.backends.meta.IndexStorage;
import com.torodb.torod.db.backends.meta.StructuresCache;
import com.torodb.torod.db.backends.meta.TorodbMeta;
import com.torodb.torod.db.backends.sql.index.UnnamedDbIndex;
import com.torodb.torod.db.backends.tables.AbstractCollectionsTable;
import com.torodb.torod.db.backends.tables.SubDocTable;

/**
 *
 */
@Singleton
public class GreenplumDatabaseInterface implements DatabaseInterface {

    private static final long serialVersionUID = 484638503;

    private final ValueToArrayConverterProvider valueToArrayConverterProvider;
    private final ValueToArrayDataTypeProvider valueToArrayDataTypeProvider;
    private final ValueToJooqConverterProvider valueToJooqConverterProvider;
    private final ValueToJooqDataTypeProvider valueToJooqDataTypeProvider;
    private final ValueToJsonConverterProvider valueToJsonConverterProvider;
    private final ScalarTypeToSqlType scalarTypeToSqlType;
    private transient @Nonnull Provider<SubDocType.Builder> subDocTypeBuilderProvider;

    private static class ArraySerializatorHolder {
        private static final ArraySerializer INSTANCE = new GreenplumJsonArraySerializer();
    }

    @Override
    public @Nonnull
    ArraySerializer arraySerializer() {
        return ArraySerializatorHolder.INSTANCE;
    }

    @Inject
    public GreenplumDatabaseInterface(ScalarTypeToSqlType scalarTypeToSqlType, Provider<Builder> subDocTypeBuilderProvider) {
        this.valueToArrayConverterProvider = GreenplumValueToArrayConverterProvider.getInstance();
        this.valueToArrayDataTypeProvider = GreenplumValueToArrayDataTypeProvider.getInstance();
        this.valueToJooqConverterProvider = GreenplumValueToJooqConverterProvider.getInstance();
        this.valueToJooqDataTypeProvider = GreenplumValueToJooqDataTypeProvider.getInstance();
        this.valueToJsonConverterProvider = GreenplumValueToJsonConverterProvider.getInstance();
        this.scalarTypeToSqlType = scalarTypeToSqlType;
        this.subDocTypeBuilderProvider = subDocTypeBuilderProvider;
    }

    private void readObject(java.io.ObjectInputStream stream)
            throws IOException, ClassNotFoundException {
        //TODO: Try to remove make DatabaseInterface not serializable
        stream.defaultReadObject();
        this.subDocTypeBuilderProvider = new SimpleSubDocTypeBuilderProvider();
    }

    @Override
    public GreenplumCollectionsTable getCollectionsTable() {
        return GreenplumCollectionsTable.COLLECTIONS;
    }

    @Override
    public StructuresCache createStructuresCache(CollectionSchema colSchema, String schemaName,
            StructureConverter converter) {
        return new GreenplumStructuresCache(colSchema, schemaName, converter);
    }

    @Override
    public IndexStorage createIndexStorage(String databaseName, CollectionSchema colSchema) {
        return new GreenplumIndexStorage(databaseName, colSchema, this);
    }

    @Override
    public ValueToArrayConverterProvider getValueToArrayConverterProvider() {
        return valueToArrayConverterProvider;
    }

    @Override
    public ValueToArrayDataTypeProvider getValueToArrayDataTypeProvider() {
        return valueToArrayDataTypeProvider;
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
    public @Nonnull ValueToJsonConverterProvider getValueToJsonConverterProvider() {
        return valueToJsonConverterProvider;
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
    public int getIntColumnType(ResultSet columns) throws SQLException {
        return columns.getInt("DATA_TYPE");
    }

    @Override
    public String getStringColumnType(ResultSet columns) throws SQLException {
        return columns.getString("TYPE_NAME");
    }

    @Override
    public ScalarTypeToSqlType getScalarTypeToSqlType() {
        return scalarTypeToSqlType;
    }

    private static @Nonnull StringBuilder fullTableName(@Nonnull String schemaName, @Nonnull String tableName) {
        return new StringBuilder()
                .append("\"").append(schemaName).append("\"")
                .append(".")
                .append("\"").append(tableName).append("\"");
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
    @Nonnull
    public UnnamedDbIndex getDbIndex(String colSchema, String tableName, Map.Entry<AttributeReference, IndexType> entrySet) {
        List<AttributeReference.Key> keys = entrySet.getKey().getKeys();

        switch (entrySet.getValue()) {
        case asc:
        case desc:
            return new UnnamedDbIndex(
                            colSchema,
                            tableName,
                            keys.get(keys.size() - 1).toString(),
                            true
                    );
        case text:
        case geospatial:
        case hashed:
        }
        throw new UnsupportedOperationException("Index of type " + entrySet.getValue() + " is not supported.");
    }
    
    @Override
    public @Nonnull String createCollectionsTableStatement(@Nonnull String schemaName, @Nonnull String tableName) {
        return new StringBuilder()
                .append("CREATE TABLE ")
                .append(fullTableName(schemaName, tableName))
                .append(" (")
                .append(AbstractCollectionsTable.TableFields.NAME.name()).append("             varchar     PRIMARY KEY     ,")
                .append(AbstractCollectionsTable.TableFields.SCHEMA.name()).append("           varchar     NOT NULL        ,")
                .append(AbstractCollectionsTable.TableFields.CAPPED.name()).append("           boolean     NOT NULL        ,")
                .append(AbstractCollectionsTable.TableFields.MAX_SIZE.name()).append("         int         NOT NULL        ,")
                .append(AbstractCollectionsTable.TableFields.MAX_ELEMENTS.name()).append("     int         NOT NULL        ,")
                .append(AbstractCollectionsTable.TableFields.OTHER.name()).append("            text                        ,")
                .append(AbstractCollectionsTable.TableFields.STORAGE_ENGINE.name()).append("   varchar     NOT NULL        ")
                .append(')')
                .toString();
    }

    @Override
    public @Nonnull String createSchemaStatement(@Nonnull String schemaName) {
        return new StringBuilder().append("CREATE SCHEMA ").append("\"").append(schemaName).append("\"").toString();
    }

    @Override
    public @Nonnull String createIndexesTableStatement(
            @Nonnull String tableName, @Nonnull String indexNameColumn, @Nonnull String indexOptionsColumn
    ) {
        return new StringBuilder()
                .append("CREATE TABLE ")
                .append(tableName)
                .append(" (")
                .append(indexNameColumn).append("       varchar     PRIMARY KEY,")
                .append(indexOptionsColumn).append("    text        NOT NULL")
                .append(")")
                .toString();
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

    public void setDeleteDidsStatementParameters(PreparedStatement ps,
            Collection<Integer> dids) throws SQLException {
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
        private final Integer typeId;
        private final Integer index;
        private final String json;
        
        private PostgresSQLFindDocsSelectStatementRow(ResultSet rs) throws SQLException {
            docid = rs.getInt(1);
            Object typeId = rs.getObject(2);
            Object index = rs.getObject(3);
            json = rs.getString(4);
            if (typeId != null) { //subdocument
                assert typeId instanceof Integer;
                assert index == null || index instanceof Integer;
                assert json != null;

                if (index == null) {
                    index = 0;
                }
                
                this.typeId = (Integer) typeId;
                this.index = (Integer) index;
            } else { //metainfo
                assert index != null;
                assert json == null;
                
                this.typeId = null;
                this.index = (Integer) index;
            }
        }
        
        @Override
        public int getDocId() {
            return docid;
        }

        @Override
        public Integer getTypeId() {
            return typeId;
        }

        @Override
        public Integer getindex() {
            return index;
        }

        @Override
        public String getJson() {
            return json;
        }

        @Override
        public boolean isSubdocument() {
            return typeId != null;
        }

        @Override
        public boolean isMetainfo() {
            return typeId == null;
        }
    };

    @Nonnull
    @Override
    public FindDocsSelectStatementRow getFindDocsSelectStatementRow(ResultSet rs) throws SQLException {
        return new PostgresSQLFindDocsSelectStatementRow(rs);
    }

    @Override
    public void setFindDocsSelectStatementParameters(CollectionSchema colSchema, Integer[] requestedDocs,
            Projection projection, Connection c, PreparedStatement ps) throws SQLException {
        ps.setString(1, colSchema.getName());

        ps.setArray(2, c.createArrayOf("integer", requestedDocs));

        Integer[] requiredTables = requiredTables(colSchema, projection);
        ps.setArray(3, c.createArrayOf("integer", requiredTables));
    }

    private Integer[] requiredTables(CollectionSchema colSchema, Projection projection) {
        Collection<SubDocTable> subDocTables = colSchema.getSubDocTables();

        Integer[] result = new Integer[subDocTables.size()];
        int i = 0;
        for (SubDocTable subDocTable : subDocTables) {
            result[i] = subDocTable.getTypeId();
            i++;
        }
        return result;
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
    public @Nonnull TorodbMeta initializeTorodbMeta(
            String databaseName, DSLContext dsl, DatabaseInterface databaseInterface
    ) throws SQLException, IOException, InvalidDatabaseException {
        return new GreenplumTorodbMeta(databaseName, dsl, databaseInterface, subDocTypeBuilderProvider);
    }

    @Override
    public void handleRetryException(SQLException sqlException) throws RetryTransactionException {
        if ("42P01".equals(sqlException.getSQLState()) && sqlException.getMessage().startsWith("ERROR: relation not found")) {
            throw new RetryTransactionException(sqlException);
        }
        if (sqlException instanceof BatchUpdateException && "40001".equals(sqlException.getSQLState())) {
            throw new RetryTransactionException(sqlException);
        }
    }

}
