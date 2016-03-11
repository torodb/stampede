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


package com.torodb.torod.db.backends.mysql;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import javax.json.Json;
import javax.json.JsonArrayBuilder;

import org.jooq.DSLContext;
import org.jooq.tools.json.JSONArray;

import com.torodb.torod.core.exceptions.ToroRuntimeException;
import com.torodb.torod.core.language.projection.Projection;
import com.torodb.torod.core.subdocument.SimpleSubDocTypeBuilderProvider;
import com.torodb.torod.core.subdocument.SubDocType;
import com.torodb.torod.core.subdocument.SubDocType.Builder;
import com.torodb.torod.db.backends.ArraySerializer;
import com.torodb.torod.db.backends.DatabaseInterface;
import com.torodb.torod.db.backends.converters.ScalarTypeToSqlType;
import com.torodb.torod.db.backends.converters.jooq.ValueToJooqConverterProvider;
import com.torodb.torod.db.backends.converters.json.ValueToJsonConverterProvider;
import com.torodb.torod.db.backends.exceptions.InvalidDatabaseException;
import com.torodb.torod.db.backends.meta.IndexStorage;
import com.torodb.torod.db.backends.meta.TorodbMeta;
import com.torodb.torod.db.backends.mysql.converters.jooq.MySQLValueToJooqConverterProvider;
import com.torodb.torod.db.backends.mysql.converters.json.MySQLValueToJsonConverterProvider;
import com.torodb.torod.db.backends.tables.CollectionsTable;
import com.torodb.torod.db.backends.tables.SubDocTable;

/**
 *
 */
@Singleton
public class MySQLDatabaseInterface implements DatabaseInterface {

    private static final long serialVersionUID = 484638503;

    private final ValueToJooqConverterProvider valueToJooqConverterProvider;
    private final ValueToJsonConverterProvider valueToJsonConverterProvider;
    private final ScalarTypeToSqlType scalarTypeToSqlType;
    private transient @Nonnull Provider<SubDocType.Builder> subDocTypeBuilderProvider;

    private static class ArraySerializatorHolder {
        private static final ArraySerializer INSTANCE = new MySQLJsonbArraySerializer();
    }

    @Override
    public @Nonnull
    ArraySerializer arraySerializer() {
        return ArraySerializatorHolder.INSTANCE;
    }

    @Inject
    public MySQLDatabaseInterface(ScalarTypeToSqlType scalarTypeToSqlType, Provider<Builder> subDocTypeBuilderProvider) {
        this.valueToJooqConverterProvider = MySQLValueToJooqConverterProvider.getInstance();
        this.valueToJsonConverterProvider = MySQLValueToJsonConverterProvider.getInstance();
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
    public @Nonnull ValueToJooqConverterProvider getValueToJooqConverterProvider() {
        return valueToJooqConverterProvider;
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
                    + "valid MySQL name. By default names must be shorter "
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
        String remarks = columns.getString("REMARKS");
        
        if (remarks != null && !remarks.isEmpty()) {
            return Types.DISTINCT;
        }
        
        return columns.getInt("DATA_TYPE");
    }

    @Override
    public String getStringColumnType(ResultSet columns) throws SQLException {
        String remarks = columns.getString("REMARKS");
        
        if (remarks != null && !remarks.isEmpty()) {
            return remarks;
        }
        
        return columns.getString("TYPE_NAME");
    }

    @Override
    public ScalarTypeToSqlType getScalarTypeToSqlType() {
        return scalarTypeToSqlType;
    }

    private static @Nonnull StringBuilder fullTableName(@Nonnull String schemaName, @Nonnull String tableName) {
        return new StringBuilder()
                .append("`").append(schemaName).append("`")
                .append(".")
                .append("`").append(tableName).append("`");
    }

    @Override
    public @Nonnull String createCollectionsTableStatement(@Nonnull String schemaName, @Nonnull String tableName) {
        return new StringBuilder()
                .append("CREATE TABLE ")
                .append(fullTableName(schemaName, tableName))
                .append(" (")
                .append(CollectionsTable.TableFields.NAME.name()).append("             varchar(3072)    PRIMARY KEY     ,")
                .append("`").append(CollectionsTable.TableFields.SCHEMA.name()).append("`").append("           varchar(64)      NOT NULL UNIQUE ,")
                .append(CollectionsTable.TableFields.CAPPED.name()).append("           boolean          NOT NULL        ,")
                .append(CollectionsTable.TableFields.MAX_SIZE.name()).append("         int              NOT NULL        ,")
                .append(CollectionsTable.TableFields.MAX_ELEMENTS.name()).append("     int              NOT NULL        ,")
                .append(CollectionsTable.TableFields.OTHER.name()).append("            json                             ,")
                .append(CollectionsTable.TableFields.STORAGE_ENGINE.name()).append("   varchar(3072)    NOT NULL         ")
                .append(")")
                .toString();
    }

    @Override
    public @Nonnull String createSchemaStatement(@Nonnull String schemaName) {
        return new StringBuilder().append("CREATE DATABASE ").append("`").append(schemaName).append("`").toString();
    }

    @Override
    public @Nonnull String createIndexesTableStatement(
            @Nonnull String tableName, @Nonnull String indexNameColumn, @Nonnull String indexOptionsColumn
    ) {
        return new StringBuilder()
                .append("CREATE TABLE ")
                .append(tableName)
                .append(" (")
                .append('`').append(indexNameColumn).append('`').append("       varchar(3072)    PRIMARY KEY,")
                .append('`').append(indexOptionsColumn).append('`').append("    json             NOT NULL    ")
                .append(")")
                .toString();
    }

    @Override
    public @Nonnull String arrayUnnestParametrizedSelectStatement() {
        //return "SELECT unnest(?)";
        throw new RuntimeException("arrays are not supported for MySQL.");
    }

    @Override
    public @Nonnull String deleteDidsStatement(
            @Nonnull String schemaName, @Nonnull String tableName,
            @Nonnull String didColumnName
    ) {
        return new StringBuilder()
                .append("DELETE FROM ")
                .append(fullTableName(schemaName, tableName))
                .append(" WHERE json_contains(?, json_extract(json_object('value', ")
                    .append(fullTableName(schemaName, tableName))
                    .append(".").append(didColumnName)
                    .append("), '$.value'))")
                .toString();
    }

    public void setDeleteDidsStatementParameters(PreparedStatement ps,
            Collection<Integer> dids) throws SQLException {
        final int maxInArray = (2 << 15) - 1; // = 2^16 -1 = 65535
        
        Integer[] didsToDelete = dids.toArray(new Integer[dids.size()]);

        int i = 0;
        while (i < didsToDelete.length) {
            int toIndex = Math.min(i + maxInArray, didsToDelete.length);
            Integer[] subDids
                    = Arrays.copyOfRange(didsToDelete, i, toIndex);
            
            JsonArrayBuilder jsonArrayBuilder = Json.createArrayBuilder();
            for (Integer subDid : subDids) {
                jsonArrayBuilder.add(subDid);
            }
            
            ps.setString(1, jsonArrayBuilder.build().toString());
            ps.addBatch();

            i = toIndex;
        }
    }

    @Override
    public @Nonnull String dropSchemaStatement(@Nonnull String schemaName) {
        return new StringBuilder()
                .append("DROP DATABASE ")
                .append("`").append(schemaName).append("`")
                .toString();
    }

    @Nonnull
    @Override
    public String findDocsSelectStatement(
    ) {
        return new StringBuilder()
                .append("CALL ")
                .append(" torodb.find_docs(?, ?, ?, ")
                .append("'ORDER BY 1 ASC'")
                .append(")")
                .toString();
    }

    @Override
    public void setFindDocsSelectStatementParameters(IndexStorage.CollectionSchema colSchema, Integer[] requestedDocs,
            Projection projection, Connection c, PreparedStatement ps) throws SQLException {
        ps.setString(1, colSchema.getName());

        ps.setString(2, JSONArray.toJSONString(Arrays.asList(requestedDocs)));

        Integer[] requiredTables = requiredTables(colSchema, projection);
        ps.setString(3, JSONArray.toJSONString(Arrays.asList(requiredTables)));
    }

    @Nonnull
    @Override
    public ResultSet getFindDocsSelectStatementResultSet(PreparedStatement ps) throws SQLException {
        if (ps.execute()) {
            return ps.getResultSet();
        }
        
        throw new ToroRuntimeException("getFindDocsSelectStatementResultSet failed");
    }

    private static class MySQLFindDocsSelectStatementRow implements FindDocsSelectStatementRow {
        private final int docid;
        private final Integer typeId;
        private final Integer index;
        private final String json;
        
        private MySQLFindDocsSelectStatementRow(ResultSet rs) throws SQLException {
            docid = rs.getInt(1);
            Object typeId = rs.getObject(2);
            Object index = rs.getObject(3);
            String json = rs.getString(4);
            if (typeId != null) { //subdocument
                assert typeId instanceof BigInteger;
                assert index == null || index instanceof Integer;
                assert json != null;

                if (index == null) {
                    index = 0;
                }
                
                this.typeId = ((BigInteger) typeId).intValue();
                this.index = (Integer) index;
                this.json = json;
            } else { //metainfo
                assert index != null;
                assert json == null;
                
                this.typeId = null;
                this.index = (Integer) index;
                this.json = null;
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
        return new MySQLFindDocsSelectStatementRow(rs);
    }

    private Integer[] requiredTables(IndexStorage.CollectionSchema colSchema, Projection projection) {
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
                .append("`").append(fullIndexName).append("`")
                .append(" ON ")
                .append("`").append(tableSchema).append("`")
                .append(".")
                .append("`").append(tableName).append("`")
                .append(" (")
                    .append("`").append(tableColumnName).append("`")
                    .append(" ").append(ascending ? "ASC" : "DESC")
                .append(")")
                .toString();
    }

    @Nonnull
    @Override
    public String dropIndexStatement(@Nonnull String schemaName, @Nonnull String indexName) {
        return new StringBuilder()
                .append("DROP INDEX ")
                .append("`").append(schemaName).append("`")
                .append(".")
                .append("`").append(indexName).append("`")
                .toString();
    }

    @Override
    public @Nonnull TorodbMeta initializeTorodbMeta(
            String databaseName, DSLContext dsl, DatabaseInterface databaseInterface
    ) throws SQLException, IOException, InvalidDatabaseException {
        return new MySQLTorodbMeta(databaseName, dsl, databaseInterface, subDocTypeBuilderProvider);
    }

}
