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

package com.torodb.core.transaction.metainf.impl.metainfo.mvcc.merge.db;

import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase.Builder;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaDatabase;
import com.torodb.core.transaction.metainf.impl.metainfo.mvcc.merge.ChildrenMergePartialStrategy;
import com.torodb.core.transaction.metainf.impl.metainfo.mvcc.merge.collection.ColContext;
import com.torodb.core.transaction.metainf.impl.metainfo.mvcc.merge.collection.CollectionMergeStrategy;
import com.torodb.core.transaction.metainf.impl.metainfo.mvcc.merge.result.ExecutionResult;
import com.torodb.core.transaction.metainf.impl.metainfo.mvcc.merge.result.ParentDescriptionFun;

import java.util.stream.Stream;

/**
 * The strategy that iterates on children elements (collections).
 */
class ModifiedDatabaseChildrenStrategy extends ChildrenMergePartialStrategy<ImmutableMetaSnapshot,
    MutableMetaDatabase, ImmutableMetaSnapshot.Builder, DbContext, Builder, ImmutableMetaDatabase>
    implements DatabasePartialStrategy {

  private final CollectionMergeStrategy collectionStrategy = new CollectionMergeStrategy();

  @Override
  public boolean appliesTo(DbContext context) {
    switch (context.getChange()) {
      case ADDED:
      case MODIFIED:
        break;
      default:
        return false;
    }

    MetaDatabase byId = getCommitedById(context);
    MetaDatabase byName = getCommitedByName(context);

    return byId != null && byId == byName;
  }

  @Override
  protected Builder createSelfBuilder(DbContext context) {
    MetaDatabase byId = getCommitedById(context);
    assert byId != null;
    return new Builder(byId.immutableCopy());
  }

  @Override
  protected Stream<ExecutionResult<ImmutableMetaDatabase>> streamChildResults(
      DbContext context, Builder selfBuilder) {
    ImmutableMetaDatabase oldVersion = getCommitedById(context);
    return context.getChanged().streamModifiedCollections()
        .map(change -> new ColContext(oldVersion, change))
        .map(ctx -> collectionStrategy.execute(ctx, selfBuilder));
  }
  
  @Override
  protected void changeParent(ImmutableMetaSnapshot.Builder parentBuilder, Builder selfBuilder) {
    parentBuilder.put(selfBuilder);
  }

  @Override
  protected String describeChanged(
      ParentDescriptionFun<ImmutableMetaSnapshot> parentDescFun,
      ImmutableMetaSnapshot parent, ImmutableMetaDatabase immutableSelf) {
    return immutableSelf.getIdentifier();
  }


}
