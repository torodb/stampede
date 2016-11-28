/*
 * ToroDB
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.backend.postgresql;

import com.torodb.backend.AbstractMetaDataReadInterface;
import com.torodb.backend.SqlHelper;
import com.torodb.backend.meta.TorodbSchema;
import com.torodb.backend.postgresql.tables.PostgreSqlKvTable;
import com.torodb.backend.postgresql.tables.PostgreSqlMetaCollectionTable;
import com.torodb.backend.postgresql.tables.PostgreSqlMetaDatabaseTable;
import com.torodb.backend.postgresql.tables.PostgreSqlMetaDocPartIndexColumnTable;
import com.torodb.backend.postgresql.tables.PostgreSqlMetaDocPartIndexTable;
import com.torodb.backend.postgresql.tables.PostgreSqlMetaDocPartTable;
import com.torodb.backend.postgresql.tables.PostgreSqlMetaFieldTable;
import com.torodb.backend.postgresql.tables.PostgreSqlMetaIndexFieldTable;
import com.torodb.backend.postgresql.tables.PostgreSqlMetaIndexTable;
import com.torodb.backend.postgresql.tables.PostgreSqlMetaScalarTable;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class PostgreSqlMetaDataReadInterface extends AbstractMetaDataReadInterface {

  private final SqlHelper sqlHelper;
  private final PostgreSqlMetaDatabaseTable metaDatabaseTable;
  private final PostgreSqlMetaCollectionTable metaCollectionTable;
  private final PostgreSqlMetaDocPartTable metaDocPartTable;
  private final PostgreSqlMetaFieldTable metaFieldTable;
  private final PostgreSqlMetaScalarTable metaScalarTable;
  private final PostgreSqlMetaDocPartIndexTable metaDocPartIndexTable;
  private final PostgreSqlMetaDocPartIndexColumnTable metaDocPartIndexColumnTable;
  private final PostgreSqlMetaIndexTable metaIndexTable;
  private final PostgreSqlMetaIndexFieldTable metaIndexFieldTable;
  private final PostgreSqlKvTable kvTable;

  @Inject
  public PostgreSqlMetaDataReadInterface(SqlHelper sqlHelper) {
    super(PostgreSqlMetaDocPartTable.DOC_PART, sqlHelper);

    this.sqlHelper = sqlHelper;
    this.metaDatabaseTable = PostgreSqlMetaDatabaseTable.DATABASE;
    this.metaCollectionTable = PostgreSqlMetaCollectionTable.COLLECTION;
    this.metaDocPartTable = PostgreSqlMetaDocPartTable.DOC_PART;
    this.metaFieldTable = PostgreSqlMetaFieldTable.FIELD;
    this.metaScalarTable = PostgreSqlMetaScalarTable.SCALAR;
    this.metaDocPartIndexTable = PostgreSqlMetaDocPartIndexTable.DOC_PART_INDEX;
    this.metaDocPartIndexColumnTable = PostgreSqlMetaDocPartIndexColumnTable.DOC_PART_INDEX_COLUMN;
    this.metaIndexTable = PostgreSqlMetaIndexTable.INDEX;
    this.metaIndexFieldTable = PostgreSqlMetaIndexFieldTable.INDEX_FIELD;
    this.kvTable = PostgreSqlKvTable.KV;
  }

  @Nonnull
  @Override
  @SuppressWarnings("unchecked")
  public PostgreSqlMetaDatabaseTable getMetaDatabaseTable() {
    return metaDatabaseTable;
  }

  @Nonnull
  @Override
  @SuppressWarnings("unchecked")
  public PostgreSqlMetaCollectionTable getMetaCollectionTable() {
    return metaCollectionTable;
  }

  @Nonnull
  @Override
  @SuppressWarnings("unchecked")
  public PostgreSqlMetaDocPartTable getMetaDocPartTable() {
    return metaDocPartTable;
  }

  @Nonnull
  @Override
  @SuppressWarnings("unchecked")
  public PostgreSqlMetaFieldTable getMetaFieldTable() {
    return metaFieldTable;
  }

  @Nonnull
  @Override
  @SuppressWarnings("unchecked")
  public PostgreSqlMetaScalarTable getMetaScalarTable() {
    return metaScalarTable;
  }

  @Nonnull
  @Override
  @SuppressWarnings("unchecked")
  public PostgreSqlMetaDocPartIndexTable getMetaDocPartIndexTable() {
    return metaDocPartIndexTable;
  }

  @Nonnull
  @Override
  @SuppressWarnings("unchecked")
  public PostgreSqlMetaDocPartIndexColumnTable getMetaDocPartIndexColumnTable() {
    return metaDocPartIndexColumnTable;
  }

  @Nonnull
  @Override
  @SuppressWarnings("unchecked")
  public PostgreSqlMetaIndexTable getMetaIndexTable() {
    return metaIndexTable;
  }

  @Nonnull
  @Override
  @SuppressWarnings("unchecked")
  public PostgreSqlMetaIndexFieldTable getMetaIndexFieldTable() {
    return metaIndexFieldTable;
  }

  @Nonnull
  @Override
  @SuppressWarnings("unchecked")
  public PostgreSqlKvTable getKvTable() {
    return kvTable;
  }

  @Override
  protected String getReadSchemaSizeStatement(String databaseName) {
    return "SELECT sum(pg_total_relation_size(quote_ident(schemaname) || '.' ||"
        + " quote_ident(tablename)))::bigint FROM pg_tables WHERE schemaname = ?";
  }

  @Override
  protected String getReadCollectionSizeStatement() {
    return "SELECT sum(pg_total_relation_size(quote_ident(schemaname) || '.' ||"
        + " quote_ident(tablename)))::bigint "
        + " FROM \"" + TorodbSchema.IDENTIFIER + "\".doc_part"
        + " LEFT JOIN pg_tables ON (tablename = doc_part.identifier)"
        + " WHERE doc_part.database = ? AND schemaname = ? AND doc_part.collection = ?";
  }

  @Override
  protected String getReadDocumentsSizeStatement() {
    return "SELECT sum(pg_total_relation_size(quote_ident(schemaname) || '.' ||"
        + " quote_ident(tablename)))::bigint "
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
        + "WHERE pg_indexes.schemaname = " + sqlHelper.renderVal(schemaName)
        + "  and pg_indexes.indexname = " + sqlHelper.renderVal(indexName)
        + ") as t";
  }
}
