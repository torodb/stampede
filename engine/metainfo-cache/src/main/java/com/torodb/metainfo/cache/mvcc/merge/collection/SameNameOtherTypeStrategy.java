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
import com.torodb.metainfo.cache.mvcc.merge.ExecutionResult;
import com.torodb.metainfo.cache.mvcc.merge.ExecutionResult.ParentDescriptionFun;


/**
 *
 */
class SameNameOtherTypeStrategy implements CollectionPartialStrategy {

  @Override
  public boolean appliesTo(ColContext context) {
    MetaCollection changed = context.getChanged();
    MetaCollection byName = getCommitedByName(context);
    return byName != null && !byName.getIdentifier().equals(changed.getIdentifier());
  }

  @Override
  public ExecutionResult<ImmutableMetaDatabase> execute(ColContext context, Builder callback) {
    return ExecutionResult.error(
        getClass(),
        parentDescFun -> getErrorMessage(parentDescFun, context)
    );
  }

  private String getErrorMessage(
      ParentDescriptionFun<ImmutableMetaDatabase> parentDescFun,
      ColContext context) {
    MetaCollection changed = context.getChanged();
    MetaCollection byName = getCommitedByName(context);
    assert byName != null;
    String parent = parentDescFun.apply(context.getCommitedParent());
    String describeChange = context.getChange().toString();

    return "The meta collection " + parent + '.' + changed.getIdentifier() + " with name "
        + changed.getName() + " cannot be " + describeChange + " because there is an already "
        + "commited collection identified as " + byName.getIdentifier() + " with the same name";
  }

}
