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
import com.torodb.core.transaction.metainf.ImmutableMetaCollection.Builder;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.metainfo.cache.mvcc.merge.ExecutionResult;
import com.torodb.metainfo.cache.mvcc.merge.ExecutionResult.ParentDescriptionFun;


/**
 *
 */
class SameIdOtherRefStrategy implements DocPartPartialStrategy {

  @Override
  public ExecutionResult<ImmutableMetaCollection> execute(
      DocPartCtx context, Builder parentBuilder) throws IllegalArgumentException {
    return ExecutionResult.error(
        getClass(),
        parentDescFun -> getErrorMessage(context, parentDescFun)
    );
  }

  @Override
  public boolean appliesTo(DocPartCtx context) {
    MetaDocPart byId = getCommitedById(context);
    return byId != null && !byId.getTableRef().equals(context.getChanged().getTableRef());
  }

  private String getErrorMessage(
      DocPartCtx context,
      ParentDescriptionFun<ImmutableMetaCollection> parentDescription) {
    MetaDocPart changed = context.getChanged();
    MetaDocPart byId = getCommitedById(context);
    assert byId != null;
    String parent = parentDescription.apply(context.getCommitedParent());

    return "There is a previous doc part on " + parent + " whose id is " + byId.getIdentifier()
        + " that has a different ref. The previous element ref is " + byId.getTableRef()
        + " and the new one is " + changed.getTableRef();
  }

}
