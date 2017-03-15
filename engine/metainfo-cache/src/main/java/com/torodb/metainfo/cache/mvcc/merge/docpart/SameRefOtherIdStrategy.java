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

package com.torodb.metainfo.cache.mvcc.merge.docpart;

import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.metainfo.cache.mvcc.merge.result.ExecutionResult;
import com.torodb.metainfo.cache.mvcc.merge.result.ParentDescriptionFun;

/**
 *
 */
class SameRefOtherIdStrategy implements DocPartPartialStrategy {

  @Override
  public boolean appliesTo(DocPartCtx context) {
    MetaDocPart byRef = getCommitedByTableRef(context);
    return byRef != null && !byRef.getIdentifier().equals(context.getChanged().getIdentifier());
  }

  @Override
  public ExecutionResult<ImmutableMetaCollection> execute(
      DocPartCtx context,
      ImmutableMetaCollection.Builder parentBuilder) throws IllegalArgumentException {
    return ExecutionResult.error(
        getClass(),
        parentDescFun -> getErrorMessage(context, parentDescFun)
    );
  }

  private String getErrorMessage(DocPartCtx context,
      ParentDescriptionFun<ImmutableMetaCollection> parentDescription) {
    MetaDocPart changed = context.getChanged();
    MetaDocPart byRef = getCommitedByTableRef(context);
    assert byRef != null;
    String parent = parentDescription.apply(context.getCommitedParent());

    return "There is a previous doc part on " + parent + " whose ref is " + byRef.getTableRef()
        + " that has a different id. The previous element id is " + byRef.getIdentifier()
        + " and the new one is " + changed.getIdentifier();

  }

}
