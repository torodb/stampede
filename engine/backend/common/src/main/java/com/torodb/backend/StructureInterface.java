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

import com.torodb.backend.converters.jooq.DataTypeForKv;
import com.torodb.backend.meta.TorodbSchema;
import com.torodb.core.TableRef;
import com.torodb.core.exceptions.InvalidDatabaseException;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaSnapshot;
import org.jooq.DSLContext;
import org.jooq.Meta;
import org.jooq.Schema;
import org.jooq.lambda.tuple.Tuple2;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

public interface StructureInterface {

  void createSchema(@Nonnull DSLContext dsl, @Nonnull String schemaName);

  void dropDatabase(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase);

  void dropCollection(@Nonnull DSLContext dsl, @Nonnull String schemaName,
      @Nonnull MetaCollection metaCollection);

  void renameCollection(@Nonnull DSLContext dsl, @Nonnull String fromSchemaName,
      @Nonnull MetaCollection fromCollection,
      @Nonnull String toSchemaName, @Nonnull MetaCollection toCollection);

  void createRootDocPartTable(@Nonnull DSLContext dsl, @Nonnull String schemaName,
      @Nonnull String tableName, @Nonnull TableRef tableRef);

  void createDocPartTable(@Nonnull DSLContext dsl, @Nonnull String schemaName,
      @Nonnull String tableName, @Nonnull TableRef tableRef,
      @Nonnull String foreignTableName);

  /**
   * Returns a stream of consumers that, when executed, creates the required indexes on a root doc
   * part table.
   *
   * The returned stream is empty if the backend is not including the internal indexes
   *
   * @param schemaName
   * @param tableName
   * @param tableRef
   * @return
   * @see DbBackend#includeInternalIndexes()
   */
  Stream<Function<DSLContext, String>> streamRootDocPartTableIndexesCreation(String schemaName,
      String tableName, TableRef tableRef);

  /**
   * Returns a stream of functions that, when executed, creates the required indexes on a doc part
   * table and return a label that indicate the type of index created.
   *
   * The returned stream is empty if the backend is not including the internal indexes
   *
   * @param schemaName
   * @param tableName
   * @param tableRef
   * @param foreignTableName
   * @return
   * @see DbBackend#includeInternalIndexes()
   */
  Stream<Function<DSLContext, String>> streamDocPartTableIndexesCreation(String schemaName,
      String tableName, TableRef tableRef, String foreignTableName);

  void addColumnToDocPartTable(@Nonnull DSLContext dsl, @Nonnull String schemaName,
      @Nonnull String tableName, @Nonnull String columnName, @Nonnull DataTypeForKv<?> dataType);

  /**
   * Returns a stream of functions that, when executed, executes backend specific tasks that should
   * be done once the data insert mode finishes and return a label that indicate the type of
   * operation executed.
   *
   * For example, PostgreSQL backend would like to run analyze on the modified tables to get some
   * stadistics.
   *
   * @param snapshot
   * @return
   */
  public Stream<Function<DSLContext, String>> streamDataInsertFinishTasks(MetaSnapshot snapshot);

  void createIndex(@Nonnull DSLContext dsl, @Nonnull String indexName, @Nonnull String tableSchema,
      @Nonnull String tableName, @Nonnull List<Tuple2<String, Boolean>> columnList, boolean unique)
      throws UserException;

  void dropIndex(@Nonnull DSLContext dsl, @Nonnull String schemaName, @Nonnull String indexName);

  /**
   * Drops all torodb elements from the backend, including metatables and their content.
   *
   * After calling this method, ToroDB cannot use the underlying backend until metada is created
   * again.
   *
   * @param dsl
   */
  void dropAll(@Nonnull DSLContext dsl);

  /**
   * Drops all user elements from the backend, including metatables content but not metatables.
   *
   * After calling this method, ToroDB sees the underlying backend as a fresh system, simmilar to
   * the one that is present the first time ToroDB starts.
   *
   * @param dsl
   */
  public void dropUserData(DSLContext dsl);

  Optional<Schema> findTorodbSchema(@Nonnull DSLContext dsl, @Nonnull Meta jooqMeta);

  default Optional<Schema> findTorodbSchema(@Nonnull DSLContext dsl) {
    return findTorodbSchema(dsl, dsl.meta());
  }

  void checkMetaDataTables(@Nonnull Schema torodbSchema) throws InvalidDatabaseException;

  default void checkMetaDataTables(@Nonnull DSLContext dsl) throws InvalidDatabaseException {
    Optional<Schema> torodbSchema = findTorodbSchema(dsl);
    if (!torodbSchema.isPresent()) {
      throw new InvalidDatabaseException("Schema '" + TorodbSchema.IDENTIFIER + "' not found");
    }
    checkMetaDataTables(torodbSchema.get());
  }
}
