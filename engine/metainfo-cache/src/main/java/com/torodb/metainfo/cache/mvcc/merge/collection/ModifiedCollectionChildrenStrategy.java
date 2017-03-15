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

import com.google.common.collect.Streams;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection.Builder;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaElementState;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.metainfo.cache.mvcc.merge.ChildrenMergePartialStrategy;
import com.torodb.metainfo.cache.mvcc.merge.ExecutionResult;
import com.torodb.metainfo.cache.mvcc.merge.docpart.DocPartCtx;
import com.torodb.metainfo.cache.mvcc.merge.docpart.DocPartMergeStrategy;
import com.torodb.metainfo.cache.mvcc.merge.index.IndexContext;
import com.torodb.metainfo.cache.mvcc.merge.index.IndexMergeStrategy;

import java.util.stream.Stream;

class ModifiedCollectionChildrenStrategy
    extends ChildrenMergePartialStrategy<ImmutableMetaDatabase, MutableMetaCollection,
      ImmutableMetaDatabase.Builder, ColContext, Builder, ImmutableMetaCollection>
    implements CollectionPartialStrategy {

  private final DocPartMergeStrategy docPartStrategy = new DocPartMergeStrategy();
  private final IndexMergeStrategy indexStrategy = new IndexMergeStrategy();

  @Override
  public boolean appliesTo(ColContext context) {
    switch (context.getChange()) {
      case ADDED:
      case MODIFIED:
        break;
      default:
        return false;
    }

    MetaCollection byId = getCommitedById(context);
    MetaCollection byName = getCommitedByName(context);

    return byId != null && byId == byName;
  }

  @Override
  protected Builder createSelfBuilder(ColContext context) {
    MetaCollection byId = getCommitedById(context);
    assert byId != null;
    return new Builder(byId.immutableCopy());
  }

  @Override
  protected Stream<ExecutionResult<ImmutableMetaCollection>> streamChildResults(
      ColContext context, Builder selfBuilder) {
    return Streams.concat(
        streamDocPartResults(context, selfBuilder),
        streamIndexResults(context, selfBuilder)
    );
  }
  
  @Override
  protected void changeParent(ImmutableMetaDatabase.Builder parentBuilder, Builder selfBuilder) {
    parentBuilder.put(selfBuilder);
  }

  @Override
  protected String describeChanged(
      ExecutionResult.ParentDescriptionFun<ImmutableMetaDatabase> parentDescFun,
      ImmutableMetaDatabase parent, ImmutableMetaCollection immutableSelf) {
    return parentDescFun.apply(parent) + '.' + immutableSelf.getIdentifier();
  }

  private Stream<ExecutionResult<ImmutableMetaCollection>> streamDocPartResults(
      ColContext context, Builder selfBuilder) {
    return context.getChanged().streamModifiedMetaDocParts()
        .map(change -> new DocPartCtx(
            getCommitedById(context),
            change,
            MetaElementState.ADDED,
            context.getChanged())
        )
        .map(childContext -> docPartStrategy.execute(childContext, selfBuilder));
  }

  private Stream<ExecutionResult<ImmutableMetaCollection>> streamIndexResults(
      ColContext context, Builder selfBuilder) {
    return context.getChanged().streamModifiedMetaIndexes()
        .map(change -> new IndexContext(
            getCommitedById(context),
            change,
            context.getChanged()
        ))
        .map(childContext -> indexStrategy.execute(childContext, selfBuilder));
  }

}
