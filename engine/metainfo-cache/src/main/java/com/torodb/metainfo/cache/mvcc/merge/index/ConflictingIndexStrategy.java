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

package com.torodb.metainfo.cache.mvcc.merge.index;

import com.torodb.core.transaction.metainf.ChangedElement;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection.Builder;
import com.torodb.core.transaction.metainf.ImmutableMetaIndex;
import com.torodb.core.transaction.metainf.MetaElementState;
import com.torodb.core.transaction.metainf.MetaIndex;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaIndex;
import com.torodb.metainfo.cache.mvcc.merge.result.ExecutionResult;
import com.torodb.metainfo.cache.mvcc.merge.result.ParentDescriptionFun;

import java.util.Optional;


/**
 *
 */
public class ConflictingIndexStrategy implements IndexPartialStrategy {

  @Override
  public boolean appliesTo(IndexContext context) {
    return getAnyConflictingIndex(context).isPresent();
  }

  @Override
  public ExecutionResult<ImmutableMetaCollection> execute(
      IndexContext context,
      Builder parentBuilder) throws IllegalArgumentException {
    
    return ExecutionResult.error(
        getClass(),
        parentDescFun -> getErrorMessage(parentDescFun, context)
    );
  }

  private String getErrorMessage(
      ParentDescriptionFun<ImmutableMetaCollection> parentDescFun,
      IndexContext context) {
    String parentDesc = parentDescFun.apply(context.getCommitedParent());
    ImmutableMetaIndex byName = context.getCommitedParent()
        .getMetaIndexByName(context.getChanged().getName());
    return "There is a previous index on " + parentDesc + "." + byName + " that conflicts with "
        + "new index " + parentDesc + '.' + context.getChanged();
  }

  public static Optional<ImmutableMetaIndex> getAnyConflictingIndex(IndexContext ctx) {
    MutableMetaCollection uncmtCol = ctx.getUncommitedParent();
    ImmutableMetaCollection cmtCol = ctx.getCommitedParent();
    MutableMetaIndex newIndex = ctx.getChanged();

    Optional<ImmutableMetaIndex> result = cmtCol.streamContainedMetaIndexes()
        .filter(index -> index.isMatch(newIndex) && uncmtCol.streamModifiedMetaIndexes()
            .noneMatch(modifiedIndex -> modifiedIndex.getChange() == MetaElementState.REMOVED
                && modifiedIndex.getElement().getName().equals(index.getName())))
        .findAny();

    //gortiz: It is very difficult to read the stream above. I am temporally adding this
    //new sentence that should be equivalent
    assert result.orElse(null) == cmtCol.streamContainedMetaIndexes()
        .filter(oldIndex -> oldIndex.isMatch(newIndex))
        .filter(oldIndex -> !hasBeenRemoved(uncmtCol, oldIndex))
        .findAny()
        .orElse(null);

    return result;
  }

  /**
   * Returns true iff the given index has been removed on the given mutable collection.
   */
  private static boolean hasBeenRemoved(MutableMetaCollection newCol, MetaIndex oldIndex) {
    Optional<ChangedElement<MutableMetaIndex>> modifiedIndex = newCol.getModifiedMetaIndexByName(
        oldIndex.getName());
    if (!modifiedIndex.isPresent()) {
      return false;
    }
    return modifiedIndex.get().getChange() == MetaElementState.REMOVED;
  }

}
