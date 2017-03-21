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

package com.torodb.d2r.metainfo.mvcc.merge.scalar;


import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart.Builder;
import com.torodb.d2r.metainfo.mvcc.merge.result.ExecutionResult;

/**
 *
 */
class NewScalarStrategy implements ScalarPartialMergeStrategy {

  @Override
  public ExecutionResult<ImmutableMetaDocPart> execute(ScalarContext context, Builder parentBuilder)
      throws IllegalArgumentException {
    parentBuilder.put(context.getChanged().immutableCopy());
    return ExecutionResult.success();
  }

  @Override
  public boolean appliesTo(ScalarContext context) {
    return getCommitedById(context) == null && getCommitedByType(context) == null;
  }

}
