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

package com.torodb.d2r.metainfo.mvcc.merge.docpart;

import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection.Builder;
import com.torodb.d2r.metainfo.mvcc.merge.result.ExecutionResult;

/**
 * The strategy used when there is no commited doc part with the same table ref and identifier.
 */
class NewDocPartStrategy implements DocPartPartialStrategy {

  @Override
  public boolean appliesTo(DocPartCtx context) {
    return getCommitedById(context) == null && getCommitedByTableRef(context) == null;
  }

  @Override
  public ExecutionResult<ImmutableMetaCollection> execute(DocPartCtx context, Builder parentBuilder)
      throws IllegalArgumentException {
    parentBuilder.put(context.getChanged().immutableCopy());
    return ExecutionResult.success();
  }

}
