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
import com.torodb.core.transaction.metainf.MetaDocPartIndex;
import com.torodb.core.transaction.metainf.MetaIndex;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.metainfo.cache.mvcc.merge.ExecutionResult;

import java.util.Optional;

/**
 * This strategy tests whether there is a {@link MetaIndex} that uses the {@link MetaDocPartIndex}
 * that is being analyzed.
 */
//TODO (gortiz): I think this test is incorrect (does not check whether the found index is
//planned to be deleted)
public class NoDocIndexStrategy implements DocPartIndexPartialStrategy {

  @Override
  public boolean appliesTo(DocPartIndexCtx context) {
    return !getAnyRelatedIndex(context).isPresent();
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
      ExecutionResult.ParentDescriptionFun<ImmutableMetaDocPart> parentDescFun,
      DocPartIndexCtx context) {
    String parentDesc = parentDescFun.apply(context.getCommitedParent());

    return "There is a new doc part index " + parentDesc + "." + context.getChanged() + " that "
        + "has no index associated";
  }

  public static Optional<? extends MetaIndex> getAnyRelatedIndex(DocPartIndexCtx context) {

    MutableMetaCollection uncommmitedCol = context.getUncommitedCollection();

    Optional<? extends MetaIndex> anyNewRelatedIndex = uncommmitedCol.streamModifiedMetaIndexes()
        .map(ChangedElement::getElement)
        .filter(newIndex -> newIndex.isCompatible(
            context.getUncommitedParent(),
            context.getChanged()))
        .findAny();

    if (anyNewRelatedIndex.isPresent()) {
      return anyNewRelatedIndex;
    }

    ImmutableMetaCollection commitedCol = context.getCommitedCollection();

    Optional<ImmutableMetaIndex> anyOldRelatedIndex = commitedCol.streamContainedMetaIndexes()
        .filter(newIndex -> newIndex.isCompatible(
            context.getUncommitedParent(),
            context.getChanged()))
        .findAny();

    return anyOldRelatedIndex;
  }
}
