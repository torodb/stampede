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

import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.metainfo.cache.mvcc.merge.result.ExecutionResult;
import com.torodb.metainfo.cache.mvcc.merge.result.ParentDescriptionFun;


/**
 *
 */
public class AnyDocPartWithMissedDocPartIndexStrategy implements IndexPartialStrategy {

  @Override
  public boolean appliesTo(IndexContext context) {
    return context.getUncommitedParent().getAnyDocPartWithMissedDocPartIndex(
        context.getCommitedParent(),
        context.getChanged()
    ).isPresent();
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
    String conflictingDocPart = context.getUncommitedParent()
        .getAnyDocPartWithMissedDocPartIndex(
            context.getCommitedParent(),
            context.getChanged()
        ).map(Object::toString)
        .orElse("unknown");

    String parentDesc = parentDescFun.apply(context.getCommitedParent());

    return "There should be a doc part index on " + parentDesc + "." + conflictingDocPart + " "
        + "associated only with new index " + context.getChanged() + " that has not been "
        + "created.";
  }

}
