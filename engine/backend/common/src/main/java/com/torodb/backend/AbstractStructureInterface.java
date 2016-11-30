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

import com.google.common.base.Preconditions;
import com.torodb.backend.ErrorHandler.Context;
import com.torodb.backend.converters.jooq.DataTypeForKv;
import com.torodb.backend.meta.TorodbSchema;
import com.torodb.backend.tables.SemanticTable;
import com.torodb.core.TableRef;
import com.torodb.core.backend.IdentifierConstraints;
import com.torodb.core.exceptions.InvalidDatabaseException;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaIdentifiedDocPartIndex;
import com.torodb.core.transaction.metainf.MetaSnapshot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Meta;
import org.jooq.Schema;
import org.jooq.Table;
import org.jooq.lambda.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public abstract class AbstractStructureInterface implements StructureInterface {

  private static final Logger LOGGER =
      LogManager.getLogger(AbstractStructureInterface.class);

  private final DbBackendService dbBackend;
  private final MetaDataReadInterface metaDataReadInterface;
  private final SqlHelper sqlHelper;
  private final IdentifierConstraints identifierConstraints;

  @Inject
  public AbstractStructureInterface(DbBackendService dbBackend,
      MetaDataReadInterface metaDataReadInterface, SqlHelper sqlHelper,
      IdentifierConstraints identifierConstraints) {
    this.dbBackend = dbBackend;
    this.metaDataReadInterface = metaDataReadInterface;
    this.sqlHelper = sqlHelper;
    this.identifierConstraints = identifierConstraints;
  }

  protected abstract void dropDatabase(DSLContext dsl, String dbIdentifier);

  @Override
  public void dropDatabase(DSLContext dsl, MetaDatabase metaDatabase) {
    Iterator<? extends MetaCollection> metaCollectionIterator = metaDatabase.streamMetaCollections()
        .iterator();
    while (metaCollectionIterator.hasNext()) {
      MetaCollection metaCollection = metaCollectionIterator.next();
      Iterator<? extends MetaDocPart> metaDocPartIterator = metaCollection
          .streamContainedMetaDocParts()
          .sorted(TableRefComparator.MetaDocPart.DESC).iterator();
      while (metaDocPartIterator.hasNext()) {
        MetaDocPart metaDocPart = metaDocPartIterator.next();
        String statement = getDropTableStatement(metaDatabase.getIdentifier(), metaDocPart
            .getIdentifier());
        sqlHelper.executeUpdate(dsl, statement, Context.DROP_TABLE);
      }
    }
    String statement = getDropSchemaStatement(metaDatabase.getIdentifier());
    sqlHelper.executeUpdate(dsl, statement, Context.DROP_SCHEMA);
  }

  @Override
  public void dropCollection(DSLContext dsl, String schemaName, MetaCollection metaCollection) {
    Iterator<? extends MetaDocPart> metaDocPartIterator = metaCollection
        .streamContainedMetaDocParts()
        .sorted(TableRefComparator.MetaDocPart.DESC).iterator();
    while (metaDocPartIterator.hasNext()) {
      MetaDocPart metaDocPart = metaDocPartIterator.next();
      String statement = getDropTableStatement(schemaName, metaDocPart.getIdentifier());
      sqlHelper.executeUpdate(dsl, statement, Context.DROP_TABLE);
    }
  }

  protected abstract String getDropTableStatement(String schemaName, String tableName);

  protected abstract String getDropSchemaStatement(String schemaName);

  @Override
  public void renameCollection(DSLContext dsl, String fromSchemaName, MetaCollection fromCollection,
      String toSchemaName, MetaCollection toCollection) {
    Iterator<? extends MetaDocPart> metaDocPartIterator = fromCollection
        .streamContainedMetaDocParts().iterator();
    while (metaDocPartIterator.hasNext()) {
      MetaDocPart fromMetaDocPart = metaDocPartIterator.next();
      MetaDocPart toMetaDocPart = toCollection.getMetaDocPartByTableRef(fromMetaDocPart
          .getTableRef());
      String renameStatement = getRenameTableStatement(fromSchemaName, fromMetaDocPart
          .getIdentifier(), toMetaDocPart.getIdentifier());
      sqlHelper.executeUpdate(dsl, renameStatement, Context.RENAME_TABLE);

      Iterator<? extends MetaIdentifiedDocPartIndex> metaDocPartIndexIterator = fromMetaDocPart
          .streamIndexes().iterator();
      while (metaDocPartIndexIterator.hasNext()) {
        MetaIdentifiedDocPartIndex fromMetaIndex = metaDocPartIndexIterator.next();
        MetaIdentifiedDocPartIndex toMetaIndex = toMetaDocPart.streamIndexes()
            .filter(index -> index.hasSameColumns(fromMetaIndex))
            .findAny()
            .get();

        String renameIndexStatement = getRenameIndexStatement(fromSchemaName, fromMetaIndex
            .getIdentifier(), toMetaIndex.getIdentifier());
        sqlHelper.executeUpdate(dsl, renameIndexStatement, Context.RENAME_INDEX);
      }

      if (!fromSchemaName.equals(toSchemaName)) {
        String setSchemaStatement = getSetTableSchemaStatement(fromSchemaName, fromMetaDocPart
            .getIdentifier(), toSchemaName);
        sqlHelper.executeUpdate(dsl, setSchemaStatement, Context.SET_TABLE_SCHEMA);
      }
    }
  }

  protected abstract String getRenameTableStatement(String fromSchemaName, String fromTableName,
      String toTableName);

  protected abstract String getRenameIndexStatement(String fromSchemaName, String fromIndexName,
      String toIndexName);

  protected abstract String getSetTableSchemaStatement(String fromSchemaName, String fromTableName,
      String toSchemaName);

  @Override
  public void createIndex(DSLContext dsl, String indexName,
      String schemaName, String tableName,
      List<Tuple2<String, Boolean>> columnList, boolean unique
  ) throws UserException {
    if (!dbBackend.isOnDataInsertMode()) {
      Preconditions.checkArgument(!columnList.isEmpty(), "Can not create index on 0 columns");

      String statement = getCreateIndexStatement(indexName, schemaName, tableName, columnList,
          unique);

      sqlHelper.executeUpdateOrThrow(dsl, statement, unique ? Context.ADD_UNIQUE_INDEX :
          Context.CREATE_INDEX);
    }
  }

  protected abstract String getCreateIndexStatement(String indexName, String schemaName,
      String tableName, List<Tuple2<String, Boolean>> columnList, boolean unique);

  @Override
  public void dropIndex(DSLContext dsl, String schemaName, String indexName) {
    String statement = getDropIndexStatement(schemaName, indexName);

    sqlHelper.executeUpdate(dsl, statement, Context.DROP_INDEX);
  }

  @Override
  public void dropAll(DSLContext dsl) {
    dropUserDatabases(dsl, metaDataReadInterface);
    metaDataReadInterface.getMetaTables().forEach(t -> dsl.dropTable(t).execute());
  }

  @Override
  public void dropUserData(DSLContext dsl) {
    dropUserDatabases(dsl, metaDataReadInterface);
    metaDataReadInterface.getMetaTables().forEach(t ->
        dsl.deleteFrom(t).execute()
    );
  }

  /**
   * This method drops all user databases (usually, db schemas).
   *
   * To implement this method, metainformation found on metatables can be acceded using the given
   * {@link MetaDataReadInterface}.
   *
   * @param dsl
   * @param metaReadInterface
   */
  protected void dropUserDatabases(DSLContext dsl, MetaDataReadInterface metaDataReadInterface) {
    metaDataReadInterface.readMetaDatabaseTable(dsl)
        .forEach(dbRecord -> dropDatabase(dsl, dbRecord.getIdentifier()));
  }

  @Override
  public Optional<Schema> findTorodbSchema(DSLContext dsl, Meta jooqMeta) {
    Schema torodbSchema = null;
    for (Schema schema : jooqMeta.getSchemas()) {
      if (identifierConstraints.isSameIdentifier(TorodbSchema.IDENTIFIER, schema.getName())) {
        torodbSchema = schema;
        break;
      }
    }
    return Optional.ofNullable(torodbSchema);
  }

  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void checkMetaDataTables(Schema torodbSchema) {

    List<SemanticTable<?>> metaTables = metaDataReadInterface.getMetaTables();
    for (SemanticTable metaTable : metaTables) {
      String metaTableName = metaTable.getName();
      boolean metaTableFound = false;
      for (Table<?> table : torodbSchema.getTables()) {
        if (identifierConstraints.isSameIdentifier(table.getName(), metaTableName)) {
          metaTable.checkSemanticallyEquals(table);
          metaTableFound = true;
          LOGGER.debug(table + " found and check");
        }
      }
      if (!metaTableFound) {
        throw new InvalidDatabaseException("The schema '" + TorodbSchema.IDENTIFIER + "'"
            + " does not contain the expected meta table '"
            + metaTableName + "'");
      }
    }

  }

  protected String getDropIndexStatement(String schemaName, String indexName) {
    StringBuilder sb = new StringBuilder()
        .append("DROP INDEX ")
        .append("\"").append(schemaName).append("\"")
        .append(".")
        .append("\"").append(indexName).append("\"");
    String statement = sb.toString();
    return statement;
  }

  @Override
  public void createSchema(DSLContext dsl, String schemaName) {
    String statement = getCreateSchemaStatement(schemaName);
    sqlHelper.executeUpdate(dsl, statement, Context.CREATE_SCHEMA);
  }

  protected abstract String getCreateSchemaStatement(String schemaName);

  @Override
  public void createRootDocPartTable(DSLContext dsl, String schemaName, String tableName,
      TableRef tableRef) {
    String statement = getCreateDocPartTableStatement(schemaName, tableName, metaDataReadInterface
        .getInternalFields(tableRef));
    sqlHelper.executeStatement(dsl, statement, Context.CREATE_TABLE);
  }

  @Override
  public void createDocPartTable(DSLContext dsl, String schemaName, String tableName,
      TableRef tableRef, String foreignTableName) {
    String statement = getCreateDocPartTableStatement(schemaName, tableName, metaDataReadInterface
        .getInternalFields(tableRef));
    sqlHelper.executeStatement(dsl, statement, Context.CREATE_TABLE);
  }

  protected abstract String getCreateDocPartTableStatement(String schemaName, String tableName,
      Collection<InternalField<?>> fields);

  @Override
  public Stream<Function<DSLContext, String>> streamRootDocPartTableIndexesCreation(
      String schemaName, String tableName, TableRef tableRef) {
    List<Function<DSLContext, String>> result = new ArrayList<>(1);
    if (!dbBackend.isOnDataInsertMode()) {
      String primaryKeyStatement = getAddDocPartTablePrimaryKeyStatement(schemaName, tableName,
          metaDataReadInterface.getPrimaryKeyInternalFields(tableRef));

      result.add(dsl -> {
        sqlHelper.executeStatement(dsl, primaryKeyStatement, Context.ADD_UNIQUE_INDEX);
        return metaDataReadInterface.getPrimaryKeyInternalFields(tableRef).stream().map(f -> f
            .getName()).collect(Collectors.joining("_")) + "_pkey";
      });
    }
    return result.stream();
  }

  @Override
  public Stream<Function<DSLContext, String>> streamDocPartTableIndexesCreation(String schemaName,
      String tableName, TableRef tableRef, String foreignTableName) {
    List<Function<DSLContext, String>> result = new ArrayList<>(4);
    if (!dbBackend.isOnDataInsertMode()) {
      String primaryKeyStatement = getAddDocPartTablePrimaryKeyStatement(schemaName, tableName,
          metaDataReadInterface.getPrimaryKeyInternalFields(tableRef));
      result.add((dsl) -> {
        sqlHelper.executeStatement(dsl, primaryKeyStatement, Context.ADD_UNIQUE_INDEX);
        return "rid_pkey";
      });
    }

    if (!dbBackend.isOnDataInsertMode()) {
      if (dbBackend.includeForeignKeys()) {
        String foreignKeyStatement = getAddDocPartTableForeignKeyStatement(schemaName, tableName,
            metaDataReadInterface.getReferenceInternalFields(tableRef),
            foreignTableName, metaDataReadInterface.getForeignInternalFields(tableRef));
        result.add((dsl) -> {
          sqlHelper.executeStatement(dsl, foreignKeyStatement, Context.ADD_FOREIGN_KEY);
          return metaDataReadInterface.getReferenceInternalFields(tableRef).stream().map(f -> f
              .getName()).collect(Collectors.joining("_")) + "_fkey";
        });
      } else {
        String foreignKeyIndexStatement = getCreateDocPartTableIndexStatement(schemaName, tableName,
            metaDataReadInterface.getReferenceInternalFields(tableRef));
        result.add((dsl) -> {
          sqlHelper.executeStatement(dsl, foreignKeyIndexStatement, Context.CREATE_INDEX);
          return metaDataReadInterface.getReferenceInternalFields(tableRef).stream().map(f -> f
              .getName()).collect(Collectors.joining("_")) + "_idx";
        });
      }
    }

    if (!dbBackend.isOnDataInsertMode()) {
      String readIndexStatement = getCreateDocPartTableIndexStatement(schemaName, tableName,
          metaDataReadInterface.getReadInternalFields(tableRef));
      result.add((dsl) -> {
        sqlHelper.executeStatement(dsl, readIndexStatement, Context.CREATE_INDEX);
        return metaDataReadInterface.getReadInternalFields(tableRef).stream()
            .map(f -> f.getName()).collect(Collectors.joining("_")) + "_idx";
      });
    }

    return result.stream();
  }

  @Override
  public Stream<Function<DSLContext, String>> streamDataInsertFinishTasks(MetaSnapshot snapshot) {
    return Collections.<Function<DSLContext, String>>emptySet().stream();
  }

  protected abstract String getAddDocPartTablePrimaryKeyStatement(String schemaName,
      String tableName,
      Collection<InternalField<?>> primaryKeyFields);

  protected abstract String getAddDocPartTableForeignKeyStatement(String schemaName,
      String tableName,
      Collection<InternalField<?>> referenceFields, String foreignTableName,
      Collection<InternalField<?>> foreignFields);

  protected abstract String getCreateDocPartTableIndexStatement(String schemaName, String tableName,
      Collection<InternalField<?>> indexedFields);

  @Override
  public void addColumnToDocPartTable(DSLContext dsl, String schemaName, String tableName,
      String columnName, DataTypeForKv<?> dataType) {
    String statement = getAddColumnToDocPartTableStatement(schemaName, tableName, columnName,
        dataType);

    sqlHelper.executeStatement(dsl, statement, Context.ADD_COLUMN);
  }

  protected abstract String getAddColumnToDocPartTableStatement(String schemaName, String tableName,
      String columnName, DataTypeForKv<?> dataType);
}
