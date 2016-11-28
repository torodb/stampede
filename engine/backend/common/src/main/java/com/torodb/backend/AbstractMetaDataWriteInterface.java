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

package com.torodb.backend;

import com.torodb.backend.ErrorHandler.Context;
import com.torodb.backend.tables.KvTable;
import com.torodb.backend.tables.MetaCollectionTable;
import com.torodb.backend.tables.MetaDatabaseTable;
import com.torodb.backend.tables.MetaDocPartIndexColumnTable;
import com.torodb.backend.tables.MetaDocPartIndexTable;
import com.torodb.backend.tables.MetaDocPartTable;
import com.torodb.backend.tables.MetaFieldTable;
import com.torodb.backend.tables.MetaIndexFieldTable;
import com.torodb.backend.tables.MetaIndexTable;
import com.torodb.backend.tables.MetaScalarTable;
import com.torodb.core.TableRef;
import com.torodb.core.backend.MetaInfoKey;
import com.torodb.core.transaction.metainf.FieldIndexOrdering;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaDocPartIndexColumn;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaIdentifiedDocPartIndex;
import com.torodb.core.transaction.metainf.MetaIndex;
import com.torodb.core.transaction.metainf.MetaIndexField;
import com.torodb.core.transaction.metainf.MetaScalar;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.TableField;
import org.jooq.conf.ParamType;

import java.util.Optional;

import javax.inject.Singleton;

@Singleton
public abstract class AbstractMetaDataWriteInterface implements MetaDataWriteInterface {

  private final MetaDatabaseTable<?> metaDatabaseTable;
  private final MetaCollectionTable<?> metaCollectionTable;
  private final MetaDocPartTable<?, ?> metaDocPartTable;
  private final MetaFieldTable<?, ?> metaFieldTable;
  private final MetaScalarTable<?, ?> metaScalarTable;
  private final MetaIndexTable<?> metaIndexTable;
  private final MetaIndexFieldTable<?, ?> metaIndexFieldTable;
  private final MetaDocPartIndexTable<?, ?> metaDocPartIndexTable;
  private final MetaDocPartIndexColumnTable<?, ?> metaDocPartIndexColumnTable;
  private final KvTable<?> kvTable;
  private final SqlHelper sqlHelper;

  public AbstractMetaDataWriteInterface(MetaDataReadInterface metaDataReadInterface,
      SqlHelper sqlHelper) {
    this.metaDatabaseTable = metaDataReadInterface.getMetaDatabaseTable();
    this.metaCollectionTable = metaDataReadInterface.getMetaCollectionTable();
    this.metaDocPartTable = metaDataReadInterface.getMetaDocPartTable();
    this.metaFieldTable = metaDataReadInterface.getMetaFieldTable();
    this.metaScalarTable = metaDataReadInterface.getMetaScalarTable();
    this.metaIndexTable = metaDataReadInterface.getMetaIndexTable();
    this.metaIndexFieldTable = metaDataReadInterface.getMetaIndexFieldTable();
    this.metaDocPartIndexTable = metaDataReadInterface.getMetaDocPartIndexTable();
    this.metaDocPartIndexColumnTable = metaDataReadInterface.getMetaDocPartIndexColumnTable();
    this.kvTable = metaDataReadInterface.getKvTable();
    this.sqlHelper = sqlHelper;
  }

  @Override
  public void createMetaDatabaseTable(DSLContext dsl) {
    String schemaName = metaDatabaseTable.getSchema().getName();
    String tableName = metaDatabaseTable.getName();
    String statement = getCreateMetaDatabaseTableStatement(schemaName, tableName);
    sqlHelper.executeStatement(dsl, statement, Context.CREATE_TABLE);
  }

  protected abstract String getCreateMetaDatabaseTableStatement(String schemaName,
      String tableName);

  @Override
  public void createMetaCollectionTable(DSLContext dsl) {
    String schemaName = metaCollectionTable.getSchema().getName();
    String tableName = metaCollectionTable.getName();
    String statement = getCreateMetaCollectionTableStatement(schemaName, tableName);
    sqlHelper.executeStatement(dsl, statement, Context.CREATE_TABLE);
  }

  protected abstract String getCreateMetaCollectionTableStatement(String schemaName,
      String tableName);

  @Override
  public void createMetaDocPartTable(DSLContext dsl) {
    String schemaName = metaDocPartTable.getSchema().getName();
    String tableName = metaDocPartTable.getName();
    String statement = getCreateMetaDocPartTableStatement(schemaName, tableName);
    sqlHelper.executeStatement(dsl, statement, Context.CREATE_TABLE);
  }

  protected abstract String getCreateMetaDocPartTableStatement(String schemaName, String tableName);

  @Override
  public void createMetaFieldTable(DSLContext dsl) {
    String schemaName = metaFieldTable.getSchema().getName();
    String tableName = metaFieldTable.getName();
    String statement = getCreateMetaFieldTableStatement(schemaName, tableName);
    sqlHelper.executeStatement(dsl, statement, Context.CREATE_TABLE);
  }

  protected abstract String getCreateMetaFieldTableStatement(String schemaName, String tableName);

  @Override
  public void createMetaScalarTable(DSLContext dsl) {
    String schemaName = metaScalarTable.getSchema().getName();
    String tableName = metaScalarTable.getName();
    String statement = getCreateMetaScalarTableStatement(schemaName, tableName);
    sqlHelper.executeStatement(dsl, statement, Context.CREATE_TABLE);
  }

  protected abstract String getCreateMetaScalarTableStatement(String schemaName, String tableName);

  @Override
  public void createMetaIndexTable(DSLContext dsl) {
    String schemaName = metaIndexTable.getSchema().getName();
    String tableName = metaIndexTable.getName();
    String statement = getCreateMetaIndexTableStatement(schemaName, tableName);
    sqlHelper.executeStatement(dsl, statement, Context.CREATE_TABLE);
  }

  protected abstract String getCreateMetaIndexTableStatement(String schemaName, String tableName);

  @Override
  public void createMetaIndexFieldTable(DSLContext dsl) {
    String schemaName = metaIndexFieldTable.getSchema().getName();
    String tableName = metaIndexFieldTable.getName();
    String statement = getCreateMetaIndexFieldTableStatement(schemaName, tableName);
    sqlHelper.executeStatement(dsl, statement, Context.CREATE_TABLE);
  }

  protected abstract String getCreateMetaIndexFieldTableStatement(String schemaName,
      String tableName);

  @Override
  public void createMetaDocPartIndexTable(DSLContext dsl) {
    String schemaName = metaDocPartIndexTable.getSchema().getName();
    String tableName = metaDocPartIndexTable.getName();
    String statement = getCreateMetaDocPartIndexTableStatement(schemaName, tableName);
    sqlHelper.executeStatement(dsl, statement, Context.CREATE_TABLE);
  }

  protected abstract String getCreateMetaDocPartIndexTableStatement(String schemaName,
      String tableName);

  @Override
  public void createMetaFieldIndexTable(DSLContext dsl) {
    String schemaName = metaDocPartIndexColumnTable.getSchema().getName();
    String tableName = metaDocPartIndexColumnTable.getName();
    String statement = getCreateMetaDocPartIndexColumnTableStatement(schemaName, tableName);
    sqlHelper.executeStatement(dsl, statement, Context.CREATE_TABLE);
  }

  protected abstract String getCreateMetaDocPartIndexColumnTableStatement(String schemaName,
      String tableName);

  @Override
  public void createKvTable(DSLContext dsl) {
    String schemaName = kvTable.getSchema().getName();
    String tableName = kvTable.getName();
    String statement = getCreateMetainfStatement(schemaName, tableName);
    sqlHelper.executeStatement(dsl, statement, Context.CREATE_TABLE);
  }

  protected abstract String getCreateMetainfStatement(String schemaName, String tableName);

  @Override
  public void addMetaDatabase(DSLContext dsl, MetaDatabase database) {
    String statement = getAddMetaDatabaseStatement(database.getName(), database.getIdentifier());
    sqlHelper.executeUpdate(dsl, statement, Context.META_INSERT);
  }

  @Override
  public void addMetaCollection(DSLContext dsl, MetaDatabase database, MetaCollection collection) {
    String statement = getAddMetaCollectionStatement(database.getName(), collection.getName(),
        collection.getIdentifier());
    sqlHelper.executeUpdate(dsl, statement, Context.META_INSERT);
  }

  @Override
  public void addMetaDocPart(DSLContext dsl, MetaDatabase database, MetaCollection collection,
      MetaDocPart docPart) {
    String statement = getAddMetaDocPartStatement(database.getName(), collection.getName(), docPart
        .getTableRef(),
        docPart.getIdentifier());
    sqlHelper.executeUpdate(dsl, statement, Context.META_INSERT);
  }

  @Override
  public void addMetaField(DSLContext dsl, MetaDatabase database, MetaCollection collection,
      MetaDocPart docPart, MetaField field) {
    String statement = getAddMetaFieldStatement(database.getName(), collection.getName(), docPart
        .getTableRef(),
        field.getName(), field.getIdentifier(),
        field.getType());
    sqlHelper.executeUpdate(dsl, statement, Context.META_INSERT);
  }

  @Override
  public void addMetaScalar(DSLContext dsl, MetaDatabase database, MetaCollection collection,
      MetaDocPart docPart, MetaScalar scalar) {
    String statement = getAddMetaScalarStatement(database.getName(), collection.getName(), docPart
        .getTableRef(),
        scalar.getIdentifier(), scalar.getType());
    sqlHelper.executeUpdate(dsl, statement, Context.META_INSERT);
  }

  @Override
  public void addMetaIndex(DSLContext dsl, MetaDatabase database, MetaCollection collection,
      MetaIndex index) {
    String statement = getAddMetaIndexStatement(database.getName(), collection.getName(), index
        .getName(),
        index.isUnique());
    sqlHelper.executeUpdate(dsl, statement, Context.META_INSERT);
  }

  @Override
  public void addMetaIndexField(DSLContext dsl, MetaDatabase database, MetaCollection collection,
      MetaIndex index, MetaIndexField field) {
    String statement = getAddMetaIndexFieldStatement(database.getName(), collection.getName(), index
        .getName(),
        field.getPosition(), field.getTableRef(), field.getName(), field.getOrdering());
    sqlHelper.executeUpdate(dsl, statement, Context.META_INSERT);
  }

  @Override
  public void addMetaDocPartIndex(DSLContext dsl, MetaDatabase database, MetaCollection collection,
      MetaDocPart docPart, MetaIdentifiedDocPartIndex index) {
    String statement = getAddMetaDocPartIndexStatement(database.getName(), index.getIdentifier(),
        collection.getName(),
        docPart.getTableRef(), index.isUnique());
    sqlHelper.executeUpdate(dsl, statement, Context.META_INSERT);
  }

  @Override
  public void addMetaDocPartIndexColumn(DSLContext dsl, MetaDatabase database,
      MetaCollection collection,
      MetaDocPart docPart, MetaIdentifiedDocPartIndex index, MetaDocPartIndexColumn column) {
    String statement = getAddMetaDocPartIndexColumnStatement(database.getName(), index
        .getIdentifier(), column.getPosition(),
        collection.getName(), docPart.getTableRef(), column.getIdentifier(), column.getOrdering());
    sqlHelper.executeUpdate(dsl, statement, Context.META_INSERT);
  }

  protected String getAddMetaDatabaseStatement(String databaseName, String databaseIdentifier) {
    String statement = sqlHelper.dsl().insertInto(metaDatabaseTable)
        .set(metaDatabaseTable.newRecord().values(databaseName, databaseIdentifier)).getSQL(
        ParamType.INLINED);
    return statement;
  }

  protected String getAddMetaCollectionStatement(String databaseName, String collectionName,
      String collectionIdentifier) {
    String statement = sqlHelper.dsl().insertInto(metaCollectionTable)
        .set(metaCollectionTable.newRecord()
            .values(databaseName, collectionName, collectionIdentifier)).getSQL(ParamType.INLINED);
    return statement;
  }

  protected String getAddMetaDocPartStatement(String databaseName, String collectionName,
      TableRef tableRef,
      String docPartIdentifier) {
    String statement = sqlHelper.dsl().insertInto(metaDocPartTable)
        .set(metaDocPartTable.newRecord()
            .values(databaseName, collectionName, tableRef, docPartIdentifier)).getSQL(
        ParamType.INLINED);
    return statement;
  }

  protected String getAddMetaFieldStatement(String databaseName, String collectionName,
      TableRef tableRef,
      String fieldName, String fieldIdentifier, FieldType type) {
    String statement = sqlHelper.dsl().insertInto(metaFieldTable)
        .set(metaFieldTable.newRecord()
            .values(databaseName, collectionName, tableRef, fieldName, type, fieldIdentifier))
        .getSQL(ParamType.INLINED);
    return statement;
  }

  protected String getAddMetaScalarStatement(String databaseName, String collectionName,
      TableRef tableRef,
      String fieldIdentifier, FieldType type) {
    String statement = sqlHelper.dsl().insertInto(metaScalarTable)
        .set(metaScalarTable.newRecord()
            .values(databaseName, collectionName, tableRef, type, fieldIdentifier)).getSQL(
        ParamType.INLINED);
    return statement;
  }

  protected String getAddMetaIndexStatement(String databaseName, String collectionName,
      String indexName, boolean unique) {
    String statement = sqlHelper.dsl().insertInto(metaIndexTable)
        .set(metaIndexTable.newRecord()
            .values(databaseName, collectionName, indexName, unique)).getSQL(ParamType.INLINED);
    return statement;
  }

  protected String getAddMetaIndexFieldStatement(String databaseName, String collectionName,
      String indexName,
      int position, TableRef tableRef, String fieldName, FieldIndexOrdering ordering) {
    String statement = sqlHelper.dsl().insertInto(metaIndexFieldTable)
        .set(metaIndexFieldTable.newRecord()
            .values(
                databaseName,
                collectionName,
                indexName,
                position,
                tableRef,
                fieldName,
                ordering))
        .getSQL(ParamType.INLINED);
    return statement;
  }

  protected String getAddMetaDocPartIndexStatement(String databaseName, String indexName,
      String collectionName,
      TableRef tableRef, boolean unique) {
    String statement = sqlHelper.dsl().insertInto(metaDocPartIndexTable)
        .set(metaDocPartIndexTable.newRecord()
            .values(databaseName, indexName, collectionName, tableRef, unique)).getSQL(
        ParamType.INLINED);
    return statement;
  }

  protected String getAddMetaDocPartIndexColumnStatement(String databaseName, String indexName,
      int position, String collectionName,
      TableRef tableRef, String columnName, FieldIndexOrdering ordering) {
    String statement = sqlHelper.dsl().insertInto(metaDocPartIndexColumnTable)
        .set(metaDocPartIndexColumnTable.newRecord()
            .values(databaseName, indexName, position, collectionName, tableRef, columnName,
                ordering)).getSQL(ParamType.INLINED);
    return statement;
  }

  @Override
  public void deleteMetaDatabase(DSLContext dsl, MetaDatabase database) {
    String statement = getDeleteMetaDatabaseStatement(database.getName());
    sqlHelper.executeUpdate(dsl, statement, Context.META_DELETE);
  }

  @Override
  public void deleteMetaCollection(DSLContext dsl, MetaDatabase database,
      MetaCollection collection) {
    String statement = getCascadeDeleteMetaDocPartIndexColumnStatement(database.getName(),
        collection.getName());
    sqlHelper.executeUpdate(dsl, statement, Context.META_DELETE);
    statement = getCascadeDeleteMetaDocPartIndexStatement(database.getName(), collection.getName());
    sqlHelper.executeUpdate(dsl, statement, Context.META_DELETE);

    statement = getCascadeDeleteMetaScalarStatement(database.getName(), collection.getName());
    sqlHelper.executeUpdate(dsl, statement, Context.META_DELETE);
    statement = getCascadeDeleteMetaFieldStatement(database.getName(), collection.getName());
    sqlHelper.executeUpdate(dsl, statement, Context.META_DELETE);
    statement = getCascadeDeleteMetaDocPartStatement(database.getName(), collection.getName());
    sqlHelper.executeUpdate(dsl, statement, Context.META_DELETE);

    statement = getCascadeDeleteMetaIndexFieldStatement(database.getName(), collection.getName());
    sqlHelper.executeUpdate(dsl, statement, Context.META_DELETE);
    statement = getCascadeDeleteMetaIndexStatement(database.getName(), collection.getName());
    sqlHelper.executeUpdate(dsl, statement, Context.META_DELETE);

    statement = getDeleteMetaCollectionStatement(database.getName(), collection.getName());
    sqlHelper.executeUpdate(dsl, statement, Context.META_DELETE);
  }

  @Override
  public void deleteMetaIndex(DSLContext dsl, MetaDatabase database, MetaCollection collection,
      MetaIndex index) {
    String statement = getCascadeDeleteMetaIndexFieldStatement(database.getName(), collection
        .getName(), index.getName());
    sqlHelper.executeUpdate(dsl, statement, Context.META_DELETE);
    statement = getDeleteMetaIndexStatement(database.getName(), collection.getName(), index
        .getName());
    sqlHelper.executeUpdate(dsl, statement, Context.META_DELETE);
  }

  @Override
  public void deleteMetaDocPartIndex(DSLContext dsl, MetaDatabase database,
      MetaCollection collection, MetaDocPart docPart, MetaIdentifiedDocPartIndex index) {
    String statement = getCascadeDeleteMetaDocPartIndexColumnStatement(database.getName(),
        collection.getName(), index.getIdentifier());
    sqlHelper.executeUpdate(dsl, statement, Context.META_DELETE);
    statement = getDeleteMetaDocPartIndexStatement(database.getName(), collection.getName(), index
        .getIdentifier());
    sqlHelper.executeUpdate(dsl, statement, Context.META_DELETE);
  }

  protected String getDeleteMetaDatabaseStatement(String databaseName) {
    String statement = sqlHelper.dsl().deleteFrom(metaDatabaseTable)
        .where(metaDatabaseTable.NAME.eq(databaseName)).getSQL(ParamType.INLINED);
    return statement;
  }

  protected String getDeleteMetaCollectionStatement(String databaseName, String collectionName) {
    String statement = sqlHelper.dsl().deleteFrom(metaCollectionTable)
        .where(metaCollectionTable.DATABASE.eq(databaseName)
            .and(metaCollectionTable.NAME.eq(collectionName))).getSQL(ParamType.INLINED);
    return statement;
  }

  protected String getCascadeDeleteMetaDocPartStatement(String databaseName,
      String collectionName) {
    String statement = sqlHelper.dsl().deleteFrom(metaDocPartTable)
        .where(metaDocPartTable.DATABASE.eq(databaseName)
            .and(metaDocPartTable.COLLECTION.eq(collectionName))).getSQL(ParamType.INLINED);
    return statement;
  }

  protected String getCascadeDeleteMetaFieldStatement(String databaseName, String collectionName) {
    String statement = sqlHelper.dsl().deleteFrom(metaFieldTable)
        .where(metaFieldTable.DATABASE.eq(databaseName)
            .and(metaFieldTable.COLLECTION.eq(collectionName))).getSQL(ParamType.INLINED);
    return statement;
  }

  protected String getCascadeDeleteMetaScalarStatement(String databaseName, String collectionName) {
    String statement = sqlHelper.dsl().deleteFrom(metaScalarTable)
        .where(metaScalarTable.DATABASE.eq(databaseName)
            .and(metaScalarTable.COLLECTION.eq(collectionName))).getSQL(ParamType.INLINED);
    return statement;
  }

  protected String getCascadeDeleteMetaIndexStatement(String databaseName, String collectionName) {
    String statement = sqlHelper.dsl().deleteFrom(metaIndexTable)
        .where(metaIndexTable.DATABASE.eq(databaseName)
            .and(metaIndexTable.COLLECTION.eq(collectionName))).getSQL(ParamType.INLINED);
    return statement;
  }

  protected String getCascadeDeleteMetaIndexFieldStatement(String databaseName,
      String collectionName) {
    String statement = sqlHelper.dsl().deleteFrom(metaIndexFieldTable)
        .where(metaIndexFieldTable.DATABASE.eq(databaseName)
            .and(metaIndexFieldTable.COLLECTION.eq(collectionName))).getSQL(ParamType.INLINED);
    return statement;
  }

  protected String getCascadeDeleteMetaIndexFieldStatement(String databaseName,
      String collectionName, String indexName) {
    String statement = sqlHelper.dsl().deleteFrom(metaIndexFieldTable)
        .where(metaIndexFieldTable.DATABASE.eq(databaseName)
            .and(metaIndexFieldTable.COLLECTION.eq(collectionName))
            .and(metaIndexFieldTable.INDEX.eq(indexName))).getSQL(ParamType.INLINED);
    return statement;
  }

  protected String getCascadeDeleteMetaDocPartIndexStatement(String databaseName,
      String collectionName) {
    String statement = sqlHelper.dsl().deleteFrom(metaDocPartIndexTable)
        .where(metaDocPartIndexTable.DATABASE.eq(databaseName)).getSQL(ParamType.INLINED);
    return statement;
  }

  protected String getCascadeDeleteMetaDocPartIndexColumnStatement(String databaseName,
      String collectionName) {
    String statement = sqlHelper.dsl().deleteFrom(metaDocPartIndexColumnTable)
        .where(metaDocPartIndexColumnTable.DATABASE.eq(databaseName)).getSQL(ParamType.INLINED);
    return statement;
  }

  protected String getCascadeDeleteMetaDocPartIndexColumnStatement(String databaseName,
      String collectionName, String indexIdentifier) {
    String statement = sqlHelper.dsl().deleteFrom(metaDocPartIndexColumnTable)
        .where(metaDocPartIndexColumnTable.DATABASE.eq(databaseName)
            .and(metaDocPartIndexColumnTable.INDEX_IDENTIFIER.eq(indexIdentifier))).getSQL(
        ParamType.INLINED);
    return statement;
  }

  protected String getDeleteMetaIndexStatement(String databaseName, String collectionName,
      String indexName) {
    String statement = sqlHelper.dsl().deleteFrom(metaIndexTable)
        .where(metaIndexTable.DATABASE.eq(databaseName)
            .and(metaIndexTable.COLLECTION.eq(collectionName))
            .and(metaIndexTable.NAME.eq(indexName))).getSQL(ParamType.INLINED);
    return statement;
  }

  protected String getDeleteMetaDocPartIndexStatement(String databaseName, String collectionName,
      String indexIdentifier) {
    String statement = sqlHelper.dsl().deleteFrom(metaDocPartIndexTable)
        .where(metaDocPartIndexTable.DATABASE.eq(databaseName)
            .and(metaDocPartIndexTable.IDENTIFIER.eq(indexIdentifier))).getSQL(ParamType.INLINED);
    return statement;
  }

  @Override
  public int consumeRids(DSLContext dsl, MetaDatabase database, MetaCollection collection,
      MetaDocPart docPart, int count) {
    Record1<Integer> lastRid = dsl.select(metaDocPartTable.LAST_RID).from(metaDocPartTable).where(
        metaDocPartTable.DATABASE.eq(database.getName())
            .and(metaDocPartTable.COLLECTION.eq(collection.getName()))
            .and(getTableRefEqCondition(metaDocPartTable.TABLE_REF, docPart.getTableRef())))
        .fetchOne();
    dsl.update(metaDocPartTable).set(metaDocPartTable.LAST_RID, metaDocPartTable.LAST_RID
        .plus(count)).where(
        metaDocPartTable.DATABASE.eq(database.getName())
            .and(metaDocPartTable.COLLECTION.eq(collection.getName()))
            .and(getTableRefEqCondition(metaDocPartTable.TABLE_REF, docPart.getTableRef())))
        .execute();
    return lastRid.value1();
  }

  protected abstract Condition getTableRefEqCondition(TableField<?, ?> field, TableRef tableRef);

  @Override
  public String writeMetaInfo(DSLContext dsl, MetaInfoKey key, String newValue) {
    Condition c = kvTable.KEY.eq(key.getKeyName());

    Optional<String> oldValue = dsl.select(kvTable.VALUE)
        .from(kvTable)
        .where(c)
        .fetchOptional()
        .map(Record1::value1);

    if (oldValue.isPresent()) {
      int updatedRows = dsl.update(kvTable)
          .set(kvTable.KEY, key.getKeyName())
          .set(kvTable.VALUE, newValue)
          .where(c)
          .execute();
      assert updatedRows == 1;
    } else {
      int newRows = dsl.insertInto(kvTable, kvTable.KEY, kvTable.VALUE)
          .values(key.getKeyName(), newValue)
          .execute();
      assert newRows == 1;
    }
    return oldValue.orElse(null);
  }
}
