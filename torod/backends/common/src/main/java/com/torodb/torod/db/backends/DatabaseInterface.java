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

package com.torodb.torod.db.backends;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.torodb.torod.core.language.projection.Projection;
import com.torodb.torod.db.backends.converters.ScalarTypeToSqlType;
import com.torodb.torod.db.backends.converters.jooq.ValueToJooqConverterProvider;
import com.torodb.torod.db.backends.converters.json.ValueToJsonConverterProvider;
import com.torodb.torod.db.backends.exceptions.InvalidDatabaseException;
import com.torodb.torod.db.backends.meta.IndexStorage;
import com.torodb.torod.db.backends.meta.TorodbMeta;
import com.torodb.torod.db.backends.query.QueryCriteriaToSQLTranslator;

/**
 * Wrapper interface to define all database-specific SQL code
 */
public interface DatabaseInterface extends Serializable {
    //TODO: Try to remove make DatabaseInterface not serializable

    @Nonnull ValueToJooqConverterProvider getValueToJooqConverterProvider();
    @Nonnull ValueToJsonConverterProvider getValueToJsonConverterProvider();
    
    @Nonnull String escapeSchemaName(@Nonnull String collection) throws IllegalArgumentException;
    @Nonnull String escapeAttributeName(@Nonnull String attributeName) throws IllegalArgumentException;
    @Nonnull String escapeIndexName(@Nonnull String indexName) throws IllegalArgumentException;

    @Nonnull int getIntColumnType(ResultSet columns) throws SQLException;
    @Nonnull String getStringColumnType(ResultSet columns) throws SQLException;
    @Nonnull ScalarTypeToSqlType getScalarTypeToSqlType();

    @Nonnull String createSchemaStatement(@Nonnull String schemaName);
    @Nonnull String createCollectionsTableStatement(@Nonnull String schemaName, @Nonnull String tableName);
    @Nonnull String createIndexesTableStatement(
            @Nonnull String tableName, @Nonnull String indexNameColumn, @Nonnull String indexOptionsColumn
    );

    @Nonnull String arrayUnnestParametrizedSelectStatement();

    @Nonnull String deleteDidsStatement(
            @Nonnull String schemaName, @Nonnull String tableName, @Nonnull String didColumnName
    );
    void setDeleteDidsStatementParameters(PreparedStatement ps,
            Collection<Integer> dids) throws SQLException;

    @Nonnull String dropSchemaStatement(@Nonnull String schemaName);

    @Nonnull String findDocsSelectStatement();

    void setFindDocsSelectStatementParameters(@Nonnull IndexStorage.CollectionSchema colSchema, @Nonnull Integer[] requestedDocs,
            @Nonnull Projection projection, @Nonnull Connection c, @Nonnull PreparedStatement ps) throws SQLException;

    @Nonnull ResultSet getFindDocsSelectStatementResultSet(PreparedStatement ps) throws SQLException;
    
    @Nonnull FindDocsSelectStatementRow getFindDocsSelectStatementRow(ResultSet rs) throws SQLException;
    
    @Nonnull String createIndexStatement(
            @Nonnull String fullIndexName, @Nonnull String tableSchema, @Nonnull String tableName,
            @Nonnull String tableColumnName, boolean isAscending
    );
    @Nonnull String dropIndexStatement(@Nonnull String schemaName, @Nonnull String indexName);

    @Nonnull ArraySerializer arraySerializer();

    @Nonnull TorodbMeta initializeTorodbMeta(String databaseName, DSLContext dsl, DatabaseInterface databaseInterface)
    throws SQLException, IOException, InvalidDatabaseException;

    public interface FindDocsSelectStatementRow {
        public int getDocId();
        public Integer getTypeId();
        public Integer getindex();
        public String getJson();
        public boolean isSubdocument();
        public boolean isMetainfo();
    }
}
