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

package com.torodb.metainfo.cache.mvcc.merge.index.field.column;

import com.google.common.collect.Lists;
import com.torodb.core.transaction.metainf.ImmutableMetaIdentifiedDocPartIndex;
import com.torodb.core.transaction.metainf.ImmutableMetaIdentifiedDocPartIndex.Builder;
import com.torodb.core.transaction.metainf.MetaDocPartIndexColumn;
import com.torodb.metainfo.cache.mvcc.merge.ExecutionResult;
import com.torodb.metainfo.cache.mvcc.merge.FirstToApplyStrategyPicker;
import com.torodb.metainfo.cache.mvcc.merge.MergeStrategy;
import com.torodb.metainfo.cache.mvcc.merge.MergeStrategyPicker;

/**
 *
 */
public class IndexColumnMergeStrategy implements MergeStrategy<ImmutableMetaIdentifiedDocPartIndex,
    MetaDocPartIndexColumn, Builder, IndexColumnCtx> {

  @SuppressWarnings("checkstyle:LineLength")
  private final MergeStrategyPicker<ImmutableMetaIdentifiedDocPartIndex, MetaDocPartIndexColumn, Builder, IndexColumnCtx> delegate;

  public IndexColumnMergeStrategy() {
    this.delegate = new FirstToApplyStrategyPicker<>(Lists.newArrayList(
        new SameIdOtherPositionNameStrategy(),
        new SamePositionOtherIdNameStrategy(),
        new NewIndexColumnStrategy()
    ));
  }

  @Override
  public ExecutionResult<ImmutableMetaIdentifiedDocPartIndex> execute(IndexColumnCtx context,
      Builder parentBuilder) {
    return delegate.pick(context)
        .execute(context, parentBuilder);
  }

}
