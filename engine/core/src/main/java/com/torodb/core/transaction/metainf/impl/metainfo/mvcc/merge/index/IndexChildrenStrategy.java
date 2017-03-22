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

package com.torodb.core.transaction.metainf.impl.metainfo.mvcc.merge.index;

import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaIndex;
import com.torodb.core.transaction.metainf.MutableMetaIndex;
import com.torodb.core.transaction.metainf.impl.metainfo.mvcc.merge.ChildrenMergePartialStrategy;
import com.torodb.core.transaction.metainf.impl.metainfo.mvcc.merge.index.field.IndexFieldContext;
import com.torodb.core.transaction.metainf.impl.metainfo.mvcc.merge.index.field.IndexFieldMergeStrategy;
import com.torodb.core.transaction.metainf.impl.metainfo.mvcc.merge.result.ExecutionResult;
import com.torodb.core.transaction.metainf.impl.metainfo.mvcc.merge.result.ParentDescriptionFun;

import java.util.stream.Stream;

public class IndexChildrenStrategy extends ChildrenMergePartialStrategy<ImmutableMetaCollection,
    MutableMetaIndex, ImmutableMetaCollection.Builder, IndexContext, ImmutableMetaIndex.Builder,
    ImmutableMetaIndex>
    implements IndexPartialStrategy {

  private final IndexFieldMergeStrategy fieldMergeStrategy = new IndexFieldMergeStrategy();

  @Override
  public boolean appliesTo(IndexContext context) {
    return getCommitedByName(context) != null;
  }

  @Override
  protected ImmutableMetaIndex.Builder createSelfBuilder(IndexContext context) {
    ImmutableMetaIndex oldByName = getCommitedByName(context);
    assert oldByName != null;

    return new ImmutableMetaIndex.Builder(oldByName);
  }

  @Override
  protected Stream<ExecutionResult<ImmutableMetaIndex>> streamChildResults(IndexContext context,
      ImmutableMetaIndex.Builder selfBuilder) {
    ImmutableMetaIndex oldVersion = getCommitedByName(context);
    return context.getChanged().streamAddedMetaIndexFields()
        .map(indexField -> new IndexFieldContext(oldVersion, indexField))
        .map(ctx -> fieldMergeStrategy.execute(ctx, selfBuilder));
  }

  @Override
  protected void changeParent(ImmutableMetaCollection.Builder parentBuilder,
      ImmutableMetaIndex.Builder selfBuilder) {
    parentBuilder.put(selfBuilder);
  }

  @Override
  protected String describeChanged(
      ParentDescriptionFun<ImmutableMetaCollection> parentDescFun,
      ImmutableMetaCollection parent, ImmutableMetaIndex immutableSelf) {
    return parentDescFun.apply(parent) + '.' + immutableSelf.getName();
  }

}
