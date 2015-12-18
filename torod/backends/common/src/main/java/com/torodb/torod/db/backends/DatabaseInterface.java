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

import com.torodb.torod.db.backends.converters.BasicTypeToSqlType;
import com.torodb.torod.db.backends.exceptions.InvalidDatabaseException;
import com.torodb.torod.db.backends.meta.TorodbMeta;
import org.jooq.DSLContext;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;

/**
 * Wrapper interface to define all database-specific SQL code
 */
public interface DatabaseInterface extends Serializable {

    @Nonnull String escapeSchemaName(@Nonnull String collection) throws IllegalArgumentException;
    @Nonnull String escapeAttributeName(@Nonnull String attributeName) throws IllegalArgumentException;
    @Nonnull String escapeIndexName(@Nonnull String indexName) throws IllegalArgumentException;

    @Nonnull BasicTypeToSqlType getBasicTypeToSqlType();

    @Nonnull String createSchemaStatement(@Nonnull String schemaName);
    @Nonnull String createCollectionsTableStatement(@Nonnull String schemaName, @Nonnull String tableName);
    @Nonnull String createIndexesTableStatement(
            @Nonnull String tableName, @Nonnull String indexNameColumn, @Nonnull String indexOptionsColumn
    );

    @Nonnull String arrayUnnestParametrizedSelectStatement();

    @Nonnull String deleteDidsStatement(
            @Nonnull String schemaName, @Nonnull String tableName, @Nonnull String didColumnName
    );

    @Nonnull String dropSchemaStatement(@Nonnull String schemaName);

    @Nonnull String findDocsSelectStatement(
            @Nonnull String didName, @Nonnull String typeIdName, @Nonnull String indexName,
            @Nonnull String jsonName
    );

    @Nonnull String createIndexStatement(
            @Nonnull String fullIndexName, @Nonnull String tableSchema, @Nonnull String tableName,
            @Nonnull String tableColumnName, boolean isAscending
    );
    @Nonnull String dropIndexStatement(@Nonnull String schemaName, @Nonnull String indexName);

    @Nonnull ArraySerializer arraySerializer();

    @Nonnull TorodbMeta initializeTorodbMeta(String databaseName, DSLContext dsl, DatabaseInterface databaseInterface)
    throws SQLException, IOException, InvalidDatabaseException;

}
