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
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot.Builder;
import com.torodb.core.transaction.metainf.MutableMetaDatabase;
import com.torodb.core.transaction.metainf.impl.metainfo.mvcc.merge.PartialMergeStrategy;

import javax.annotation.Nullable;


/**
 * A marker interface created to simplify the declaration of collection sub strategies.
 */
interface DatabasePartialStrategy extends
    PartialMergeStrategy<ImmutableMetaSnapshot, MutableMetaDatabase, Builder, DbContext> {

  @Nullable
  default ImmutableMetaDatabase getCommitedById(DbContext context) {
    return context.getCommitedParent()
        .getMetaDatabaseByIdentifier(context.getChanged().getIdentifier());
  }

  @Nullable
  default ImmutableMetaDatabase getCommitedByName(DbContext context) {
    return context.getCommitedParent().getMetaDatabaseByName(
        context.getChanged().getName()
    );
  }

}
