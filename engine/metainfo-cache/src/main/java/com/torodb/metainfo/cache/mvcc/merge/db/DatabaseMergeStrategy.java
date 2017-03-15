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

package com.torodb.metainfo.cache.mvcc.merge.db;

import com.google.common.collect.Lists;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot.Builder;
import com.torodb.core.transaction.metainf.MutableMetaDatabase;
import com.torodb.metainfo.cache.mvcc.merge.ByStateStrategyPicker;
import com.torodb.metainfo.cache.mvcc.merge.DoNothingMergeStrategy;
import com.torodb.metainfo.cache.mvcc.merge.FirstToApplyStrategyPicker;
import com.torodb.metainfo.cache.mvcc.merge.MergeStrategy;
import com.torodb.metainfo.cache.mvcc.merge.MergeStrategyPicker;
import com.torodb.metainfo.cache.mvcc.merge.result.ExecutionResult;

/**
 * The root strategy used to merge databases.
 */
@SuppressWarnings("checkstyle:LineLength")
public class DatabaseMergeStrategy
    implements MergeStrategy<ImmutableMetaSnapshot, MutableMetaDatabase, Builder, DbContext> {

  private final ByStateStrategyPicker<ImmutableMetaSnapshot, MutableMetaDatabase, Builder, DbContext> delegate;

  public DatabaseMergeStrategy() {
    this.delegate = new ByStateStrategyPicker<>(
        createOnAddStrategy(),
        createOnModifyStrategy(),
        createOnRemoveStrategy()
    );
  }

  @Override
  public ExecutionResult<ImmutableMetaSnapshot> execute(DbContext context, Builder parentBuilder) {
    return delegate.pick(context)
        .execute(context, parentBuilder);
  }

  private static MergeStrategyPicker<ImmutableMetaSnapshot, MutableMetaDatabase, Builder, DbContext> createOnAddStrategy() {
    return new FirstToApplyStrategyPicker<>(Lists.newArrayList(
        new SameIdOtherNameStrategy(),
        new SameNameOtherTypeStrategy(),
        new NewDatabaseStrategy(),
        new ShortcutDatabaseStrategy(),
        new ModifiedDatabaseChildrenStrategy()
    ), new DoNothingMergeStrategy<>());
  }

  private static MergeStrategyPicker<ImmutableMetaSnapshot, MutableMetaDatabase, Builder, DbContext> createOnModifyStrategy() {
    return createOnAddStrategy();
  }

  private static MergeStrategyPicker<ImmutableMetaSnapshot, MutableMetaDatabase, Builder, DbContext> createOnRemoveStrategy() {
    return new FirstToApplyStrategyPicker<ImmutableMetaSnapshot, MutableMetaDatabase, Builder, DbContext>(Lists.newArrayList(
        new SameIdOtherNameStrategy(),
        new SameNameOtherTypeStrategy(),
        new ShortcutDatabaseStrategy(),
        new NotExistentDatabaseStrategy()
    ), DatabaseMergeStrategy::deleteDatabase);
  }

  private static ExecutionResult<ImmutableMetaSnapshot> deleteDatabase(
      DbContext context,
      Builder parentBuilder) {
    parentBuilder.remove(context.getChanged());
    return ExecutionResult.success();
  }

}
