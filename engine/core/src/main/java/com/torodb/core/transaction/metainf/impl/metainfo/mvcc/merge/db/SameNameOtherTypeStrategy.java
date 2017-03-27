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

import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.impl.metainfo.mvcc.merge.result.ExecutionResult;
import com.torodb.core.transaction.metainf.impl.metainfo.mvcc.merge.result.ParentDescriptionFun;


/**
 * Checks whether there is a commited database with the same name but different id.
 */
class SameNameOtherTypeStrategy implements DatabasePartialStrategy {

  @Override
  public boolean appliesTo(DbContext context) {
    MetaDatabase changed = context.getChanged();
    MetaDatabase byName = getCommitedByName(context);
    return byName != null && !byName.getIdentifier().equals(changed.getIdentifier());
  }

  @Override
  public ExecutionResult<ImmutableMetaSnapshot> execute(
      DbContext context,
      ImmutableMetaSnapshot.Builder callback)
      throws IllegalArgumentException {
    return ExecutionResult.error(
        getClass(),
        parentDescFun -> getErrorMessage(parentDescFun, context)
    );
  }

  private String getErrorMessage(
      ParentDescriptionFun<ImmutableMetaSnapshot> parentDescFun,
      DbContext context) {
    MetaDatabase changed = context.getChanged();
    MetaDatabase byName = getCommitedByName(context);
    assert byName != null;
    String parent = parentDescFun.apply(context.getCommitedParent());
    String describeChange = context.getChange().toString();

    return "The meta database " + parent + '.' + changed.getIdentifier() + " with name "
        + changed.getName() + " cannot be " + describeChange + " because there is an already "
        + "commited database identified as " + byName.getIdentifier() + " with the same name";
  }

}
