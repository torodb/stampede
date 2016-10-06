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
import com.torodb.backend.meta.TorodbSchema;
import com.torodb.backend.postgresql.tables.PostgreSQLMetaDocPartIndexTable;
import com.torodb.backend.postgresql.tables.PostgreSQLMetaFieldIndexTable;
import com.torodb.backend.postgresql.tables.PostgreSQLMetaIndexFieldTable;
import com.torodb.backend.postgresql.tables.PostgreSQLMetaIndexTable;
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
    private final PostgreSQLMetaDocPartIndexTable metaDocPartIndexTable;
    private final PostgreSQLMetaFieldIndexTable metaFieldIndexTable;
    private final PostgreSQLMetaIndexTable metaIndexTable;
    private final PostgreSQLMetaIndexFieldTable metaIndexFieldTable;

    @Inject
    public PostgreSQLMetaDataReadInterface(SqlHelper sqlHelper) {
        super(PostgreSQLMetaDocPartTable.DOC_PART, sqlHelper);
        
        this.sqlHelper = sqlHelper;
        this.metaDatabaseTable = PostgreSQLMetaDatabaseTable.DATABASE;
        this.metaCollectionTable = PostgreSQLMetaCollectionTable.COLLECTION;
        this.metaDocPartTable = PostgreSQLMetaDocPartTable.DOC_PART;
        this.metaFieldTable = PostgreSQLMetaFieldTable.FIELD;
        this.metaScalarTable = PostgreSQLMetaScalarTable.SCALAR;
        this.metaDocPartIndexTable = PostgreSQLMetaDocPartIndexTable.DOC_PART_INDEX;
        this.metaFieldIndexTable = PostgreSQLMetaFieldIndexTable.FIELD_INDEX;
        this.metaIndexTable = PostgreSQLMetaIndexTable.INDEX;
        this.metaIndexFieldTable = PostgreSQLMetaIndexFieldTable.INDEX_FIELD;
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

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public PostgreSQLMetaDocPartIndexTable getMetaDocPartIndexTable() {
        return metaDocPartIndexTable;
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public PostgreSQLMetaFieldIndexTable getMetaFieldIndexTable() {
        return metaFieldIndexTable;
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public PostgreSQLMetaIndexTable getMetaIndexTable() {
        return metaIndexTable;
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public PostgreSQLMetaIndexFieldTable getMetaIndexFieldTable() {
        return metaIndexFieldTable;
    }
    
    @Override
    protected String getReadSchemaSizeStatement(String databaseName) {
        return "SELECT sum(pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename)))::bigint FROM pg_tables WHERE schemaname = ?";
    }

    @Override
    protected String getReadCollectionSizeStatement() {
        return "SELECT sum(pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename)))::bigint "
                + " FROM \"" + TorodbSchema.IDENTIFIER + "\".doc_part"
                + " LEFT JOIN pg_tables ON (tablename = doc_part.identifier)"
                + " WHERE doc_part.database = ? AND schemaname = ? AND doc_part.collection = ?";
    }

    @Override
    protected String getReadDocumentsSizeStatement() {
        return "SELECT sum(pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename)))::bigint "
                + " FROM \"" + TorodbSchema.IDENTIFIER + "\".doc_part"
                + " LEFT JOIN pg_tables ON (tablename = doc_part.identifier)"
                + " WHERE doc_part.database = ? AND schemaname = ? AND doc_part.collection = ?";
    }

    @Override
    protected String getReadIndexSizeStatement(
            String schemaName, String tableName, String indexName) {
        return "SELECT sum(table_size)::bigint from ("
                + "SELECT pg_relation_size(pg_class.oid) AS table_size "
                + "FROM pg_class join pg_indexes "
                + "  on pg_class.relname = pg_indexes.tablename "
                + "WHERE pg_indexes.schemaname = "  + sqlHelper.renderVal(schemaName)
                + "  and pg_indexes.indexname = " + sqlHelper.renderVal(indexName)
                + ") as t";
    }
}
