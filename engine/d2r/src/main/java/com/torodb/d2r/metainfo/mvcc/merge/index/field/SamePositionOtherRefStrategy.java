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

package com.torodb.d2r.metainfo.mvcc.merge.index.field;

import com.torodb.core.transaction.metainf.ImmutableMetaIndex;
import com.torodb.core.transaction.metainf.ImmutableMetaIndex.Builder;
import com.torodb.core.transaction.metainf.MetaIndexField;
import com.torodb.d2r.metainfo.mvcc.merge.result.ExecutionResult;
import com.torodb.d2r.metainfo.mvcc.merge.result.ParentDescriptionFun;



/**
 *
 */
class SamePositionOtherRefStrategy implements IndexFieldPartialStrategy {

  @Override
  public boolean appliesTo(IndexFieldContext context) {
    MetaIndexField byPos = getCommitedByPosition(context);
    return byPos != null && !byPos.getTableRef().equals(context.getChanged().getTableRef());
  }

  @Override
  public ExecutionResult<ImmutableMetaIndex> execute(
      IndexFieldContext context,
      Builder parentBuilder) throws IllegalArgumentException {
    return ExecutionResult.error(
        getClass(),
        parentDescFun -> getErrorMessage(context, parentDescFun)
    );
  }

  private String getErrorMessage(IndexFieldContext context,
      ParentDescriptionFun<ImmutableMetaIndex> parentDescription) {
    MetaIndexField changed = context.getChanged();
    MetaIndexField byPos = getCommitedByPosition(context);
    assert byPos != null;
    String parent = parentDescription.apply(context.getCommitedParent());
    String describeChange = context.getChange().toString();

    return "The index " + parent + " contains a new field with name " + changed.getFieldName() + " "
        + "on " + changed.getTableRef() + " at position " + changed.getPosition() + " that cannot "
        + "be " + describeChange + " because there is an already commited field with the position "
        + "whose table ref is " + byPos.getTableRef();
  }

}
