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

package com.torodb.core.transaction.metainf.impl.metainfo.mvcc.merge.collection;

import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase.Builder;
import com.torodb.core.transaction.metainf.MetaElementState;
import com.torodb.core.transaction.metainf.impl.metainfo.mvcc.merge.result.ExecutionResult;

/**
 * The strategy used when there is no commited collection with the same name and identifier.
 */
class NewCollectionStrategy implements CollectionPartialStrategy {

  @Override
  public boolean appliesTo(ColContext context) {
    MetaElementState change = context.getChange();
    if (change != MetaElementState.ADDED && change != MetaElementState.MODIFIED) {
      return false;
    }
    return getCommitedById(context) == null && getCommitedByName(context) == null;
  }

  @Override
  public ExecutionResult<ImmutableMetaDatabase> execute(ColContext context, Builder parentBuilder) {
    parentBuilder.put(context.getChanged().immutableCopy());
    return ExecutionResult.success();
  }

}
