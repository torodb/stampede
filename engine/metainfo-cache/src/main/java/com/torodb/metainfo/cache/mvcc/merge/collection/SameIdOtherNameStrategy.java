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
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.metainfo.cache.mvcc.merge.result.ExecutionResult;
import com.torodb.metainfo.cache.mvcc.merge.result.ParentDescriptionFun;

/**
 * Checks whether there is a commited collection with the same id but different name.
 */
class SameIdOtherNameStrategy implements CollectionPartialStrategy {

  @Override
  public boolean appliesTo(ColContext context) {
    MetaCollection byId = getCommitedById(context);
    return byId != null && !byId.getName().equals(context.getChanged().getName());
  }

  @Override
  public ExecutionResult<ImmutableMetaDatabase> execute(
      ColContext context,
      Builder parentBuilder) throws IllegalArgumentException {
    return ExecutionResult.error(
        getClass(),
        parentDescFun -> getErrorMessage(context, parentDescFun)
    );
  }

  private String getErrorMessage(ColContext context, 
      ParentDescriptionFun<ImmutableMetaDatabase> parentDescription) {
    MetaCollection changed = context.getChanged();
    MetaCollection byId = getCommitedById(context);
    assert byId != null;
    String parent = parentDescription.apply(context.getCommitedParent());
    String describeChange = context.getChange().toString();

    return "The modified meta collection " + parent + '.' + changed.getIdentifier() + " with name "
        + changed.getName() + " cannot be " + describeChange + " because there is an already "
        + "commited collection identified as " + byId.getIdentifier() + " whose name is "
        + byId.getName();
  }

}
