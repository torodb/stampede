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

package com.torodb.metainfo.cache.mvcc.merge.index.field.column;

import com.torodb.core.transaction.metainf.ImmutableMetaIdentifiedDocPartIndex;
import com.torodb.core.transaction.metainf.MetaDocPartIndexColumn;
import com.torodb.metainfo.cache.mvcc.merge.ExecutionResult;
import com.torodb.metainfo.cache.mvcc.merge.ExecutionResult.ParentDescriptionFun;

/**
 *
 */
class SamePositionOtherIdNameStrategy implements IndexColumnPartialStrategy {

  @Override
  public boolean appliesTo(IndexColumnCtx context) {
    MetaDocPartIndexColumn byPos = getCommitedByPosition(context);
    return byPos != null && !byPos.getIdentifier().equals(context.getChanged().getIdentifier());
  }

  @Override
  public ExecutionResult<ImmutableMetaIdentifiedDocPartIndex> execute(IndexColumnCtx context,
      ImmutableMetaIdentifiedDocPartIndex.Builder parentBuilder) throws IllegalArgumentException {
    return ExecutionResult.error(
        getClass(),
        parentDescFun -> getErrorMessage(context, parentDescFun)
    );
  }

  private String getErrorMessage(IndexColumnCtx context,
      ParentDescriptionFun<ImmutableMetaIdentifiedDocPartIndex> parentDescription) {
    MetaDocPartIndexColumn changed = context.getChanged();
    MetaDocPartIndexColumn byPos = getCommitedByPosition(context);
    assert byPos != null;
    String parent = parentDescription.apply(context.getCommitedParent());
    String describeChange = context.getChange().toString();

    return "The index field " + parent + " contains indexes a column identified as "
        + changed.getIdentifier() + " at position " + changed.getPosition() + " that cannot "
        + "be " + describeChange + " because at that position there is an already commited column "
        + "whose identifier is " + byPos.getIdentifier();
  }

}
