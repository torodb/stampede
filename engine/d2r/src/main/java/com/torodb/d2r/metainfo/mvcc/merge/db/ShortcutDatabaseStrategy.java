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

package com.torodb.d2r.metainfo.mvcc.merge.db;

import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MutableMetaDatabase;
import com.torodb.d2r.metainfo.mvcc.merge.result.ExecutionResult;

/**
 * A strategy that shortcuts the merge process if the commited database is the same as the
 * {@link MutableMetaDatabase#getOrigin() origin} of the one that is being merged, which means
 * that no other transaction modified the commited database and therefore no more checks need
 * to be done.
 */
public class ShortcutDatabaseStrategy implements DatabasePartialStrategy {

  @Override
  public boolean appliesTo(DbContext context) {
    ImmutableMetaDatabase origin = context.getChanged().getOrigin();
    ImmutableMetaDatabase commited = context.getCommitedParent()
        .getMetaDatabaseByName(origin.getName());

    return commited != null && commited == origin;
  }

  @Override
  public ExecutionResult<ImmutableMetaSnapshot> execute(DbContext context,
      ImmutableMetaSnapshot.Builder parentBuilder) throws IllegalArgumentException {

    switch (context.getChange()) {
      case ADDED:
      case MODIFIED:
        parentBuilder.put(context.getChanged().immutableCopy());
        break;
      case REMOVED:
        parentBuilder.remove(context.getChanged());
        break;
      default:
        throw new AssertionError("Unexpected change " + context.getChange());
    }

    return ExecutionResult.success();
  }

}
