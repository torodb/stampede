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

package com.torodb.metainfo.cache.mvcc.merge.db;

import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot.Builder;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.metainfo.cache.mvcc.merge.result.ExecutionResult;
import com.torodb.metainfo.cache.mvcc.merge.result.ParentDescriptionFun;

/**
 * Checks whether there is a commited database with the same id but different name.
 */
class SameIdOtherNameStrategy implements DatabasePartialStrategy {

  @Override
  public boolean appliesTo(DbContext context) {
    MetaDatabase byId = getCommitedById(context);
    return byId != null && !byId.getName().equals(context.getChanged().getName());
  }

  @Override
  public ExecutionResult<ImmutableMetaSnapshot> execute(DbContext context, Builder parentBuilder)
      throws IllegalArgumentException {
    return ExecutionResult.error(
        getClass(),
        parentDescFun -> getErrorMessage(context, parentDescFun)
    );
  }

  private String getErrorMessage(DbContext context,
      ParentDescriptionFun<ImmutableMetaSnapshot> parentDescription) {
    MetaDatabase changed = context.getChanged();
    MetaDatabase byId = getCommitedById(context);
    assert byId != null;
    String parent = parentDescription.apply(context.getCommitedParent());
    String describeChange = context.getChange().toString();

    return "The modified meta database " + parent + '.' + changed.getIdentifier() + " with name "
        + changed.getName() + " cannot be " + describeChange + " because there is an already "
        + "commited database identified as " + byId.getIdentifier() + " whose name is "
        + byId.getName();
  }

}
