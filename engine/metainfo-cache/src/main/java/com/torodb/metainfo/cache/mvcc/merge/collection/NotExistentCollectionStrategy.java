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

import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase.Builder;
import com.torodb.core.transaction.metainf.MetaElementState;
import com.torodb.metainfo.cache.mvcc.merge.ExecutionResult;

/**
 * A partial strategy that applies when a collection is being removed but it is not found on the
 * commited database.
 *
 * <p/>This can happen when a collection is created and removed on the same transaction or when
 * merging a transaction that removes a collection that has been already removed by a concurrent
 * transaction that has been commited after this one is created but before it is commited.
 */
class NotExistentCollectionStrategy implements CollectionPartialStrategy {

  @Override
  public boolean appliesTo(ColContext context) {
    MetaElementState change = context.getChange();
    if (change != MetaElementState.REMOVED) {
      return false;
    }
    return getCommitedById(context) == null && getCommitedByName(context) == null;
  }

  @Override
  public ExecutionResult<ImmutableMetaDatabase> execute(ColContext context,
      Builder parentBuilder) throws IllegalArgumentException {
    return ExecutionResult.success();
  }

}
