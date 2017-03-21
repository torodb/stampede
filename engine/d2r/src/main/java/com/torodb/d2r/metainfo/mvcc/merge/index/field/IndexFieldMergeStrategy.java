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

package com.torodb.d2r.metainfo.mvcc.merge.index.field;

import com.google.common.collect.Lists;
import com.torodb.core.transaction.metainf.ImmutableMetaIndex;
import com.torodb.core.transaction.metainf.MetaIndexField;
import com.torodb.d2r.metainfo.mvcc.merge.FirstToApplyStrategyPicker;
import com.torodb.d2r.metainfo.mvcc.merge.MergeStrategy;
import com.torodb.d2r.metainfo.mvcc.merge.MergeStrategyPicker;
import com.torodb.d2r.metainfo.mvcc.merge.result.ExecutionResult;

public class IndexFieldMergeStrategy implements MergeStrategy<ImmutableMetaIndex, MetaIndexField,
    ImmutableMetaIndex.Builder, IndexFieldContext> {

  @SuppressWarnings("checkstyle:LineLength")
  private final MergeStrategyPicker<ImmutableMetaIndex, MetaIndexField, ImmutableMetaIndex.Builder, IndexFieldContext> delegate;

  public IndexFieldMergeStrategy() {
    this.delegate = new FirstToApplyStrategyPicker<>(Lists.newArrayList(
        new NewIndexFieldStrategy(),
        new SamePositionOtherFieldNameStrategy(),
        new SamePositionOtherRefStrategy(),
        new SameRefAndNameOtherPositionStrategy()
    ));
  }

  @Override
  public ExecutionResult<ImmutableMetaIndex> execute(IndexFieldContext context,
      ImmutableMetaIndex.Builder parentBuilder) {
    return delegate.pick(context)
        .execute(context, parentBuilder);
  }

}
