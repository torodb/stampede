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

import com.torodb.torod.core.subdocument.SimpleSubDocTypeBuilderProvider;
import com.torodb.torod.core.subdocument.SubDocType;
import com.torodb.torod.core.subdocument.SubDocType.Builder;
import com.torodb.torod.db.backends.ArraySerializer;
import com.torodb.torod.db.backends.DatabaseInterface;
import com.torodb.torod.db.backends.converters.ScalarTypeToSqlType;
import com.torodb.torod.db.backends.exceptions.InvalidDatabaseException;
import com.torodb.torod.db.backends.meta.TorodbMeta;
import com.torodb.torod.db.backends.tables.CollectionsTable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import org.jooq.DSLContext;

/**
 *
 */
@Singleton
public class PostgresqlDatabaseInterface implements DatabaseInterface {

    private static final long serialVersionUID = 484638503;

    private final ScalarTypeToSqlType scalarTypeToSqlType;
    private transient @Nonnull Provider<SubDocType.Builder> subDocTypeBuilderProvider;

    private static class ArraySerializatorHolder {
        private static final ArraySerializer INSTANCE = new JsonbArraySerializer();
    }

    @Override
    public @Nonnull
    ArraySerializer arraySerializer() {
        return ArraySerializatorHolder.INSTANCE;
    }

    @Inject
    public PostgresqlDatabaseInterface(ScalarTypeToSqlType scalarTypeToSqlType, Provider<Builder> subDocTypeBuilderProvider) {
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
    public @Nonnull String createCollectionsTableStatement(@Nonnull String schemaName, @Nonnull String tableName) {
        return new StringBuilder()
                .append("CREATE TABLE ")
                .append(fullTableName(schemaName, tableName))
                .append(" (")
                .append(CollectionsTable.TableFields.NAME.name()).append("             varchar     PRIMARY KEY     ,")
                .append(CollectionsTable.TableFields.SCHEMA.name()).append("           varchar     NOT NULL UNIQUE ,")
                .append(CollectionsTable.TableFields.CAPPED.name()).append("           boolean     NOT NULL        ,")
                .append(CollectionsTable.TableFields.MAX_SIZE.name()).append("         int         NOT NULL        ,")
                .append(CollectionsTable.TableFields.MAX_ELEMENTS.name()).append("     int         NOT NULL        ,")
                .append(CollectionsTable.TableFields.OTHER.name()).append("            jsonb                       ,")
                .append(CollectionsTable.TableFields.STORAGE_ENGINE.name()).append("   varchar     NOT NULL        ")
                .append(")")
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
                .append(indexOptionsColumn).append("    jsonb       NOT NULL")
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
    public String findDocsSelectStatement(
            @Nonnull String didName, @Nonnull String typeIdName, @Nonnull String indexName, @Nonnull String jsonName
    ) {
        return new StringBuilder()
                .append("SELECT ")
                .append(didName).append(", ")
                .append(typeIdName).append(", ")
                .append(indexName).append(", ")
                .append(jsonName)
                .append(" FROM torodb.find_docs(?, ?, ?) ORDER BY ")
                .append(didName).append(" ASC")
                .toString();
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
    public @Nonnull TorodbMeta initializeTorodbMeta(
            String databaseName, DSLContext dsl, DatabaseInterface databaseInterface
    ) throws SQLException, IOException, InvalidDatabaseException {
        return new PostgreSQLTorodbMeta(databaseName, dsl, databaseInterface, subDocTypeBuilderProvider);
    }

}
