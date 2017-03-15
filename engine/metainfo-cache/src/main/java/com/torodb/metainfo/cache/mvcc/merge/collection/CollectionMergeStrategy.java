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

package com.torodb.metainfo.cache.mvcc.merge.collection;

import com.google.common.collect.Lists;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase.Builder;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.metainfo.cache.mvcc.merge.ByStateStrategyPicker;
import com.torodb.metainfo.cache.mvcc.merge.DoNothingMergeStrategy;
import com.torodb.metainfo.cache.mvcc.merge.FirstToApplyStrategyPicker;
import com.torodb.metainfo.cache.mvcc.merge.MergeContext;
import com.torodb.metainfo.cache.mvcc.merge.MergeStrategy;
import com.torodb.metainfo.cache.mvcc.merge.MergeStrategyPicker;
import com.torodb.metainfo.cache.mvcc.merge.result.ExecutionResult;

/**
 * The root strategy used to merge collections.
 */
@SuppressWarnings("checkstyle:LineLength")
public class CollectionMergeStrategy implements MergeStrategy<ImmutableMetaDatabase,
    MutableMetaCollection, Builder, ColContext> {

  private final ByStateStrategyPicker<ImmutableMetaDatabase, MutableMetaCollection, Builder, ColContext> delegate;

  public CollectionMergeStrategy() {
    this.delegate = new ByStateStrategyPicker<>(
        createOnAddStrategy(),
        createOnModifyStrategy(),
        createOnRemoveStrategy()
    );
  }

  @Override
  public ExecutionResult<ImmutableMetaDatabase> execute(ColContext context, Builder parentBuilder) {
    return delegate.pick(context)
        .execute(context, parentBuilder);
  }

  private static MergeStrategyPicker<ImmutableMetaDatabase, MutableMetaCollection, Builder, ColContext> createOnAddStrategy() {
    return new FirstToApplyStrategyPicker<>(Lists.newArrayList(
        new SameIdOtherNameStrategy(),
        new SameNameOtherIdStrategy(),
        new NewCollectionStrategy(),
        new ShortcutCollectionStrategy(),
        new ModifiedCollectionChildrenStrategy()
    ), new DoNothingMergeStrategy<>());
  }

  private static MergeStrategyPicker<ImmutableMetaDatabase, MutableMetaCollection, Builder, ColContext> createOnModifyStrategy() {
    return createOnAddStrategy();
  }

  private static MergeStrategyPicker<ImmutableMetaDatabase, MutableMetaCollection, Builder, ColContext> createOnRemoveStrategy() {
    return new FirstToApplyStrategyPicker<ImmutableMetaDatabase,
        MutableMetaCollection, Builder, ColContext>(Lists.newArrayList(
        new SameIdOtherNameStrategy(),
        new SameNameOtherIdStrategy(),
        new NotExistentCollectionStrategy(),
        new ShortcutCollectionStrategy()
    ), CollectionMergeStrategy::deleteCollection);
  }

  private static ExecutionResult deleteCollection(
      MergeContext<ImmutableMetaDatabase, MutableMetaCollection> context, Builder parentBuilder) {
    parentBuilder.remove(context.getChanged());
    return ExecutionResult.success();
  }

}
