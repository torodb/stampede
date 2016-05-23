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

package com.torodb.poc.backend;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.DataType;
import org.jooq.Field;

import com.torodb.poc.backend.converters.jooq.ValueToJooqConverterProvider;
import com.torodb.poc.backend.converters.jooq.ValueToJooqDataTypeProvider;
import com.torodb.poc.backend.sql.index.NamedDbIndex;
import com.torodb.poc.backend.tables.CollectionTable;
import com.torodb.poc.backend.tables.PathDocTable;
import com.torodb.poc.backend.tables.records.FieldRecord;
import com.torodb.torod.core.connection.exceptions.RetryTransactionException;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;

/**
 * Wrapper interface to define all database-specific SQL code
 */
public interface DatabaseInterface extends Serializable {
    //TODO: Try to remove make DatabaseInterface not serializable
    @Nonnull CollectionTable<?> getCollectionTable();

    @Nonnull ValueToJooqConverterProvider getValueToJooqConverterProvider();
    @Nonnull ValueToJooqDataTypeProvider getValueToJooqDataTypeProvider();
    
    @Nonnull String escapeSchemaName(@Nonnull String collection) throws IllegalArgumentException;
    @Nonnull String escapeAttributeName(@Nonnull String attributeName) throws IllegalArgumentException;
    @Nonnull String escapeIndexName(@Nonnull String indexName) throws IllegalArgumentException;

    @Nonnull Iterable<FieldRecord> getFields(String database, String collection, String path);
    @Nonnull DataType<?> getDataType(String type);
    
    @Nonnull ResultSet getColumns(DatabaseMetaData metadata, String schemaName, String tableName) throws SQLException;
    @Nonnull ResultSet getIndexes(DatabaseMetaData metadata, String schemaName, String tableName) throws SQLException;
    @Nonnull int getIntColumnType(ResultSet columns) throws SQLException;
    @Nonnull String getStringColumnType(ResultSet columns) throws SQLException;
    @Nonnull ScalarTypeToSqlType getScalarTypeToSqlType();

    @Nonnull String createSchemaStatement(@Nonnull String schemaName);
    @Nonnull String dropSchemaStatement(@Nonnull String schemaName);
    @Nonnull String createDatabaseTableStatement(@Nonnull String schemaName, @Nonnull String tableName);
    @Nonnull String createCollectionTableStatement(@Nonnull String schemaName, @Nonnull String tableName);
    @Nonnull String createContainerTableStatement(@Nonnull String schemaName, @Nonnull String tableName);
    @Nonnull String createFieldTableStatement(@Nonnull String schemaName, @Nonnull String tableName);
    @Nonnull String createIndexesTableStatement(@Nonnull String schemaName, @Nonnull String tableName, @Nonnull String indexNameColumn, @Nonnull String indexOptionsColumn);
    @Nonnull String addColumnsToTableStatement(@Nonnull String schemaName, @Nonnull String tableName, @Nonnull List<Field<?>> fields, @Nonnull Configuration conf);
    
    @Nonnull String getCreateIndexQuery(PathDocTable table, Field<?> field, Configuration conf);
    @Nonnull String getCreateSubDocTypeTableQuery(String schemaName, String tableName, List<Field<?>> fields, Configuration conf);
    
    void insertRootDocuments(@Nonnull DSLContext dsl, @Nonnull String schema, @Nonnull String collection, @Nonnull Collection<SplitDocument> docs, Configuration conf) throws ImplementationDbException;
    long getDatabaseSize(@Nonnull DSLContext dsl, @Nonnull String databaseName);
    Long getCollectionSize(@Nonnull DSLContext dsl, @Nonnull String schema, @Nonnull String collection);
    Long getDocumentsSize(@Nonnull DSLContext dsl, @Nonnull String schema, String collection);
    Long getIndexSize(@Nonnull DSLContext dsl, @Nonnull String schema, String collection, String index, Set<NamedDbIndex> relatedDbIndexes, Map<String, Integer> relatedToroIndexes);
    void insertPathDocuments(DSLContext dsl, String schema, PathDocTable table, List<Field<?>> fields, Iterable<PathDocument> pathDocuments, Configuration conf);
    
    @Nonnull String arrayUnnestParametrizedSelectStatement();

    @Nonnull String deleteDidsStatement(@Nonnull String schemaName, @Nonnull String tableName, @Nonnull String didColumnName);
    void setDeleteDidsStatementParameters(PreparedStatement ps, Collection<Integer> dids) throws SQLException;
    
    @Nonnull String createIndexStatement(@Nonnull String fullIndexName, @Nonnull String tableSchema, 
            @Nonnull String tableName, @Nonnull String tableColumnName, boolean isAscending);
    @Nonnull String dropIndexStatement(@Nonnull String schemaName, @Nonnull String indexName);

    @Nonnull String findDocsSelectStatement();
    void setFindDocsSelectStatementParameters(@Nonnull String schema, @Nonnull Integer[] requestedDocs,
            @Nonnull String[] paths, @Nonnull Connection c, @Nonnull PreparedStatement ps) throws SQLException;
    @Nonnull ResultSet getFindDocsSelectStatementResultSet(PreparedStatement ps) throws SQLException;
    @Nonnull FindDocsSelectStatementRow getFindDocsSelectStatementRow(ResultSet rs) throws SQLException;
    
    public interface FindDocsSelectStatementRow {
        public int getDocId();
        public Integer getRowId();
        public Integer getParentRowId();
        public Integer getSequence();
        public String getJson();
        public boolean isRoot();
        public boolean isObject();
    }

    void handleRetryException(SQLException sqlException) throws RetryTransactionException;
}
