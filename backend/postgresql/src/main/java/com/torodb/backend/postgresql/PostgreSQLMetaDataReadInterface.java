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

package com.torodb.backend.postgresql;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.torodb.backend.AbstractMetaDataReadInterface;
import com.torodb.backend.SqlHelper;
import com.torodb.backend.index.NamedDbIndex;
import com.torodb.backend.postgresql.tables.PostgreSQLMetaCollectionTable;
import com.torodb.backend.postgresql.tables.PostgreSQLMetaDatabaseTable;
import com.torodb.backend.postgresql.tables.PostgreSQLMetaDocPartTable;
import com.torodb.backend.postgresql.tables.PostgreSQLMetaFieldTable;
import com.torodb.backend.postgresql.tables.PostgreSQLMetaScalarTable;

/**
 *
 */
@Singleton
public class PostgreSQLMetaDataReadInterface extends AbstractMetaDataReadInterface {

    private final SqlHelper sqlHelper;
    private final PostgreSQLMetaDatabaseTable metaDatabaseTable;
    private final PostgreSQLMetaCollectionTable metaCollectionTable;
    private final PostgreSQLMetaDocPartTable metaDocPartTable;
    private final PostgreSQLMetaFieldTable metaFieldTable;
    private final PostgreSQLMetaScalarTable metaScalarTable;

    @Inject
    public PostgreSQLMetaDataReadInterface(SqlHelper sqlHelper) {
        super(PostgreSQLMetaDocPartTable.DOC_PART, sqlHelper);
        
        this.sqlHelper = sqlHelper;
        this.metaDatabaseTable = PostgreSQLMetaDatabaseTable.DATABASE;
        this.metaCollectionTable = PostgreSQLMetaCollectionTable.COLLECTION;
        this.metaDocPartTable = PostgreSQLMetaDocPartTable.DOC_PART;
        this.metaFieldTable = PostgreSQLMetaFieldTable.FIELD;
        this.metaScalarTable = PostgreSQLMetaScalarTable.SCALAR;
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public PostgreSQLMetaDatabaseTable getMetaDatabaseTable() {
        return metaDatabaseTable;
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public PostgreSQLMetaCollectionTable getMetaCollectionTable() {
        return metaCollectionTable;
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public PostgreSQLMetaDocPartTable getMetaDocPartTable() {
        return metaDocPartTable;
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public PostgreSQLMetaFieldTable getMetaFieldTable() {
        return metaFieldTable;
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public PostgreSQLMetaScalarTable getMetaScalarTable() {
        return metaScalarTable;
    }
    
    @Override
    protected String getReadDatabaseSizeStatement(String databaseName) {
        return "SELECT sum(pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename)))::bigint FROM pg_tables WHERE schemaname = ?";
    }

    @Override
    protected String getReadCollectionSizeStatement(String schema, String collection) {
        return "SELECT sum(table_size)::bigint "
                + "FROM ("
                + "  SELECT "
                + "    pg_relation_size(pg_catalog.pg_class.oid) as table_size "
                + "  FROM pg_catalog.pg_class "
                + "    JOIN pg_catalog.pg_namespace "
                + "       ON relnamespace = pg_catalog.pg_namespace.oid "
                + "    WHERE pg_catalog.pg_namespace.nspname = " + sqlHelper.renderVal(schema)
                + ") AS t";
    }

    @Override
    protected String getReadDocumentsSizeStatement(String schema, String collection) {
        return "SELECT sum(table_size)::bigint from ("
                + "SELECT pg_relation_size(pg_class.oid) AS table_size "
                + "FROM pg_class join pg_tables on pg_class.relname = pg_tables.tablename "
                + "where pg_tables.schemaname = " + sqlHelper.renderVal(schema)
                + "   and pg_tables.tablename LIKE 't_%'"
                + ") as t";
    }

    @Override
    protected String getReadIndexSizeStatement(String schema, String collection, String index,
            Set<NamedDbIndex> relatedDbIndexes, Map<String, Integer> relatedToroIndexes) {
        return "SELECT sum(table_size)::bigint from ("
                + "SELECT pg_relation_size(pg_class.oid) AS table_size "
                + "FROM pg_class join pg_indexes "
                + "  on pg_class.relname = pg_indexes.tablename "
                + "WHERE pg_indexes.schemaname = "  + sqlHelper.renderVal(schema)
                + "  and pg_indexes.indexname = " + sqlHelper.renderVal(index)
                + ") as t";
    }
}
