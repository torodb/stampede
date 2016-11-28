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

package com.torodb.backend.derby;

import com.torodb.backend.AbstractMetaDataReadInterface;
import com.torodb.backend.SqlHelper;
import com.torodb.backend.derby.tables.DerbyKvTable;
import com.torodb.backend.derby.tables.DerbyMetaCollectionTable;
import com.torodb.backend.derby.tables.DerbyMetaDatabaseTable;
import com.torodb.backend.derby.tables.DerbyMetaDocPartIndexColumnTable;
import com.torodb.backend.derby.tables.DerbyMetaDocPartIndexTable;
import com.torodb.backend.derby.tables.DerbyMetaDocPartTable;
import com.torodb.backend.derby.tables.DerbyMetaFieldTable;
import com.torodb.backend.derby.tables.DerbyMetaIndexFieldTable;
import com.torodb.backend.derby.tables.DerbyMetaIndexTable;
import com.torodb.backend.derby.tables.DerbyMetaScalarTable;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 *
 */
@Singleton
public class DerbyMetaDataReadInterface extends AbstractMetaDataReadInterface {

  private final DerbyMetaDatabaseTable metaDatabaseTable;
  private final DerbyMetaCollectionTable metaCollectionTable;
  private final DerbyMetaDocPartTable metaDocPartTable;
  private final DerbyMetaFieldTable metaFieldTable;
  private final DerbyMetaScalarTable metaScalarTable;
  private final DerbyMetaDocPartIndexTable metaDocPartIndexTable;
  private final DerbyMetaDocPartIndexColumnTable metaFieldIndexTable;
  private final DerbyMetaIndexTable metaIndexTable;
  private final DerbyMetaIndexFieldTable metaIndexFieldTable;
  private final DerbyKvTable kvTable;

  @Inject
  public DerbyMetaDataReadInterface(SqlHelper sqlHelper) {
    super(DerbyMetaDocPartTable.DOC_PART, sqlHelper);

    this.metaDatabaseTable = DerbyMetaDatabaseTable.DATABASE;
    this.metaCollectionTable = DerbyMetaCollectionTable.COLLECTION;
    this.metaDocPartTable = DerbyMetaDocPartTable.DOC_PART;
    this.metaFieldTable = DerbyMetaFieldTable.FIELD;
    this.metaScalarTable = DerbyMetaScalarTable.SCALAR;
    this.metaDocPartIndexTable = DerbyMetaDocPartIndexTable.DOC_PART_INDEX;
    this.metaFieldIndexTable = DerbyMetaDocPartIndexColumnTable.DOC_PART_INDEX_COLUMN;
    this.metaIndexTable = DerbyMetaIndexTable.INDEX;
    this.metaIndexFieldTable = DerbyMetaIndexFieldTable.INDEX_FIELD;
    this.kvTable = DerbyKvTable.KV;
  }

  @Nonnull
  @Override
  @SuppressWarnings("unchecked")
  public DerbyMetaDatabaseTable getMetaDatabaseTable() {
    return metaDatabaseTable;
  }

  @Nonnull
  @Override
  @SuppressWarnings("unchecked")
  public DerbyMetaCollectionTable getMetaCollectionTable() {
    return metaCollectionTable;
  }

  @Nonnull
  @Override
  @SuppressWarnings("unchecked")
  public DerbyMetaDocPartTable getMetaDocPartTable() {
    return metaDocPartTable;
  }

  @Nonnull
  @Override
  @SuppressWarnings("unchecked")
  public DerbyMetaFieldTable getMetaFieldTable() {
    return metaFieldTable;
  }

  @Nonnull
  @Override
  @SuppressWarnings("unchecked")
  public DerbyMetaScalarTable getMetaScalarTable() {
    return metaScalarTable;
  }

  @Nonnull
  @Override
  @SuppressWarnings("unchecked")
  public DerbyMetaDocPartIndexTable getMetaDocPartIndexTable() {
    return metaDocPartIndexTable;
  }

  @Nonnull
  @Override
  @SuppressWarnings("unchecked")
  public DerbyMetaDocPartIndexColumnTable getMetaDocPartIndexColumnTable() {
    return metaFieldIndexTable;
  }

  @Nonnull
  @Override
  @SuppressWarnings("unchecked")
  public DerbyMetaIndexTable getMetaIndexTable() {
    return metaIndexTable;
  }

  @Nonnull
  @Override
  @SuppressWarnings("unchecked")
  public DerbyMetaIndexFieldTable getMetaIndexFieldTable() {
    return metaIndexFieldTable;
  }

  @Nonnull
  @Override
  @SuppressWarnings("unchecked")
  public DerbyKvTable getKvTable() {
    return kvTable;
  }

  @Override
  protected String getReadSchemaSizeStatement(String databaseName) {
    return "SELECT 0 FROM SYSIBM.SYSDUMMY1 WHERE ? IS NOT NULL";
  }

  @Override
  protected String getReadCollectionSizeStatement() {
    return "SELECT 0 FROM SYSIBM.SYSDUMMY1 WHERE ? IS NOT NULL AND ? IS NOT NULL AND ? IS NOT NULL";
  }

  @Override
  protected String getReadDocumentsSizeStatement() {
    return "SELECT 0 FROM SYSIBM.SYSDUMMY1 WHERE ? IS NOT NULL AND ? IS NOT NULL AND ? IS NOT NULL";
  }

  @Override
  protected String getReadIndexSizeStatement(
      String schemaName, String tableName, String indexName) {
    throw new UnsupportedOperationException();
  }
}
