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

package com.torodb.core.transaction.metainf.impl.metainfo.mvcc.merge.index;

import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.MetaIdentifiedDocPartIndex;
import com.torodb.core.transaction.metainf.impl.metainfo.mvcc.merge.result.ExecutionResult;
import com.torodb.core.transaction.metainf.impl.metainfo.mvcc.merge.result.ParentDescriptionFun;

import java.util.Optional;

public class OrphanDocPartIndexStrategy implements IndexPartialStrategy {

  @Override
  public boolean appliesTo(IndexContext context) {
    return getOrphan(context).isPresent();
  }

  @Override
  public ExecutionResult<ImmutableMetaCollection> execute(IndexContext context,
      ImmutableMetaCollection.Builder parentBuilder) throws IllegalArgumentException {

    return ExecutionResult.error(
        getClass(),
        parentDescFun -> getErrorMessage(parentDescFun, context)
    );
  }

  private String getErrorMessage(
      ParentDescriptionFun<ImmutableMetaCollection> parentDescFun,
      IndexContext context) {
    String parentDesc = parentDescFun.apply(context.getCommitedParent());
    String orphan = getOrphan(context)
        .map(MetaIdentifiedDocPartIndex::getIdentifier)
        .orElse("unknown");
    return "There is a previous doc part index on " + parentDesc + "." + orphan + " associated "
        + "only with removed index " + context.getChanged() + " that has not been deleted.";
  }

  private Optional<? extends MetaIdentifiedDocPartIndex> getOrphan(IndexContext context) {
    return context.getUncommitedParent().getAnyOrphanDocPartIndex(
        context.getCommitedParent(),
        context.getChanged()
    );
  }

}
