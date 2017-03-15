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

package com.torodb.metainfo.cache.mvcc.merge.docpartindex;

import com.google.common.collect.Lists;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart.Builder;
import com.torodb.core.transaction.metainf.MetaIdentifiedDocPartIndex;
import com.torodb.metainfo.cache.mvcc.merge.ByStateStrategyPicker;
import com.torodb.metainfo.cache.mvcc.merge.FirstToApplyStrategyPicker;
import com.torodb.metainfo.cache.mvcc.merge.MergeStrategy;
import com.torodb.metainfo.cache.mvcc.merge.MergeStrategyPicker;
import com.torodb.metainfo.cache.mvcc.merge.result.ExecutionResult;

public class DocPartIndexMergeStrategy implements
    MergeStrategy<ImmutableMetaDocPart, MetaIdentifiedDocPartIndex, Builder, DocPartIndexCtx> {

  private final MergeStrategyPicker<ImmutableMetaDocPart, MetaIdentifiedDocPartIndex, Builder,
      DocPartIndexCtx> delegate;

  public DocPartIndexMergeStrategy() {
    delegate = new ByStateStrategyPicker<>(
        createOnAddStrategy(),
        createOnModifyStrategy(),
        createOnRemoveStrategy()
    );
  }

  @Override
  public ExecutionResult<ImmutableMetaDocPart> execute(DocPartIndexCtx context,
      Builder parentBuilder) {
    return delegate.pick(context)
        .execute(context, parentBuilder);
  }

  private static MergeStrategyPicker<ImmutableMetaDocPart, MetaIdentifiedDocPartIndex, Builder,
      DocPartIndexCtx> createOnAddStrategy() {
    return new FirstToApplyStrategyPicker<>(Lists.newArrayList(
        new NoDocIndexStrategy(),
        new NewDocPartIndexStrategy(),
        new DocPartIndexChildrenStrategy()
    ));
  }

  @SuppressWarnings("checkstyle:LineLength")
  private static MergeStrategyPicker<ImmutableMetaDocPart, MetaIdentifiedDocPartIndex, Builder,
      DocPartIndexCtx> createOnModifyStrategy() {
    return createOnAddStrategy();
  }

  @SuppressWarnings("checkstyle:LineLength")
  private static MergeStrategyPicker<ImmutableMetaDocPart, MetaIdentifiedDocPartIndex, Builder,
      DocPartIndexCtx> createOnRemoveStrategy() {
    return new FirstToApplyStrategyPicker<>(Lists.newArrayList(
        new MissedDocIndexStrategy(),
        new NotExistentDocPartIndexStrategy()
    ), DocPartIndexMergeStrategy::deleteIndex);
  }

  private static ExecutionResult<ImmutableMetaDocPart> deleteIndex(
      DocPartIndexCtx ctx, Builder parentBuilder) {
    parentBuilder.remove(ctx.getChanged());
    return ExecutionResult.success();
  }


}
