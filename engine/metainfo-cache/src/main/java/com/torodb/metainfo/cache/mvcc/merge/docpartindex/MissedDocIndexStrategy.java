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

package com.torodb.metainfo.cache.mvcc.merge.docpartindex;

import com.torodb.core.transaction.metainf.ChangedElement;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.ImmutableMetaIndex;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDocPartIndex;
import com.torodb.core.transaction.metainf.MetaElementState;
import com.torodb.core.transaction.metainf.MetaIdentifiedDocPartIndex;
import com.torodb.core.transaction.metainf.MetaIndex;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaIndex;
import com.torodb.metainfo.cache.mvcc.merge.result.ExecutionResult;
import com.torodb.metainfo.cache.mvcc.merge.result.ParentDescriptionFun;

import java.util.Optional;

/**
 * This strategy tests whether there is a {@link MetaIndex} that uses the {@link MetaDocPartIndex}
 * that is being deleted.
 */
public class MissedDocIndexStrategy implements DocPartIndexPartialStrategy {

  @Override
  public boolean appliesTo(DocPartIndexCtx context) {
    if (context.getChange() != MetaElementState.REMOVED) {
      return false;
    }
    return getAnyMissedIndex(context).isPresent();
  }

  @Override
  public ExecutionResult<ImmutableMetaDocPart> execute(DocPartIndexCtx context,
      ImmutableMetaDocPart.Builder parentBuilder) throws IllegalArgumentException {

    return ExecutionResult.error(
        getClass(),
        parentDescFun -> getErrorMessage(parentDescFun, context)
    );
  }

  private String getErrorMessage(
      ParentDescriptionFun<ImmutableMetaDocPart> parentDescFun,
      DocPartIndexCtx context) {
    String parentDesc = parentDescFun.apply(context.getCommitedParent());
    String missedIndex = getAnyMissedIndex(context)
        .map(MetaIndex::getName)
        .orElse("unknown");

    return "The already commited index " + missedIndex + " is compatible with the removed "
        + "doc part index " + parentDesc + "." + context.getChanged().getIdentifier();
  }

  /**
   * Looks for a {@link MetaIndex} that is compatible with the removed {@link MetaDocPartIndex} and
   * hasn't been removed.
   */
  public static Optional<ImmutableMetaIndex> getAnyMissedIndex(DocPartIndexCtx ctx) {

    ImmutableMetaCollection oldCol = ctx.getCommitedCollection();
    MutableMetaCollection newCol = ctx.getUncommitedCollection();
    MetaIdentifiedDocPartIndex changed = ctx.getChanged();

    Optional<ImmutableMetaIndex> result = oldCol.streamContainedMetaIndexes()
        .flatMap(oldIndex -> oldIndex.streamTableRefs()
            .map(tableRef -> oldCol.getMetaDocPartByTableRef(tableRef))
            .filter(oldDocPart -> oldDocPart != null && oldIndex.isCompatible(oldDocPart,
                changed) && newCol.streamModifiedMetaIndexes()
                    .noneMatch(newIndex -> newIndex.getChange() == MetaElementState.REMOVED
                        && newIndex.getElement().getName().equals(oldIndex.getName())))
            .map(tableRef -> oldIndex))
        .findAny();

    //gortiz: It is very difficult to read the stream above. I am temporally adding this
    //new sentence that should be equivalent
    assert result.orElse(null) == oldCol.streamContainedMetaIndexes()
        .filter(oldIndex -> areCompatible(oldCol, oldIndex, changed))
        .filter(oldIndex -> !hasBeenRemoved(newCol, oldIndex))
        .findAny()
        .orElse(null);

    return result;
  }

  /**
   * Returns true iff the given {@link MetaIndex} is compatible with the given 
   * {@link MetaIdentifiedDocPartIndex} on the given {@link MetaCollection}.
   */
  private static boolean areCompatible(MetaCollection oldCol, MetaIndex oldIndex,
      MetaIdentifiedDocPartIndex changed) {
    return oldIndex.streamTableRefs()
        .map(tableRef -> oldCol.getMetaDocPartByTableRef(tableRef))
        .filter(oldDocPart -> oldDocPart != null)
        .anyMatch(oldDocPart -> oldIndex.isCompatible(oldDocPart, changed));
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
