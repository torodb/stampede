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

package com.torodb.metainfo.cache.mvcc.merge.index;


import com.google.common.collect.Lists;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection.Builder;
import com.torodb.core.transaction.metainf.MutableMetaIndex;
import com.torodb.metainfo.cache.mvcc.merge.ByStateStrategyPicker;
import com.torodb.metainfo.cache.mvcc.merge.FirstToApplyStrategyPicker;
import com.torodb.metainfo.cache.mvcc.merge.MergeStrategy;
import com.torodb.metainfo.cache.mvcc.merge.MergeStrategyPicker;
import com.torodb.metainfo.cache.mvcc.merge.result.ExecutionResult;

public class IndexMergeStrategy
    implements MergeStrategy<ImmutableMetaCollection, MutableMetaIndex, Builder, IndexContext> {

  @SuppressWarnings("checkstyle:LineLength")
  private final MergeStrategyPicker<ImmutableMetaCollection, MutableMetaIndex, Builder, IndexContext> delegate;

  public IndexMergeStrategy() {
    this.delegate = new ByStateStrategyPicker<>(
        createOnAddStrategy(),
        createOnModifyStrategy(),
        createOnRemoveStrategy()
    );
  }

  @Override
  public ExecutionResult<ImmutableMetaCollection> execute(IndexContext ctx, Builder parentBuilder) {
    return delegate.pick(ctx)
        .execute(ctx, parentBuilder);
  }

  @SuppressWarnings("checkstyle:LineLength")
  private static MergeStrategyPicker<ImmutableMetaCollection, MutableMetaIndex, Builder, IndexContext> createOnAddStrategy() {
    return new FirstToApplyStrategyPicker<>(Lists.newArrayList(
        new ConflictingIndexStrategy(),
        new AnyDocPartWithMissedDocPartIndexStrategy(),
        new NewIndexStrategy(),
        new IndexChildrenStrategy()
    ));
  }

  @SuppressWarnings("checkstyle:LineLength")
  private static MergeStrategyPicker<ImmutableMetaCollection, MutableMetaIndex, Builder, IndexContext> createOnModifyStrategy() {
    return createOnAddStrategy();
  }

  @SuppressWarnings("checkstyle:LineLength")
  private static MergeStrategyPicker<ImmutableMetaCollection, MutableMetaIndex, Builder, IndexContext> createOnRemoveStrategy() {
    return new FirstToApplyStrategyPicker<>(Lists.newArrayList(
        new OrphanDocPartIndexStrategy(),
        new NotExistentIndexStrategy()
    ), IndexMergeStrategy::deleteIndex);
  }

  private static ExecutionResult<ImmutableMetaCollection> deleteIndex(
      IndexContext ctx, Builder parentBuilder) {
    parentBuilder.remove(ctx.getChanged());
    return ExecutionResult.success();
  }

}
