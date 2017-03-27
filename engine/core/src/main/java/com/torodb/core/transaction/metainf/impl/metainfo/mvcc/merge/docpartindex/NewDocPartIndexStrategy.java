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

package com.torodb.core.transaction.metainf.impl.metainfo.mvcc.merge.docpartindex;

import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.impl.metainfo.mvcc.merge.result.ExecutionResult;

/**
 * The strategy used when there is no commited collection with the identifier.
 */
public class NewDocPartIndexStrategy implements DocPartIndexPartialStrategy {

  @Override
  public boolean appliesTo(DocPartIndexCtx context) {
    return getCommitedById(context) == null;
  }

  @Override
  public ExecutionResult<ImmutableMetaDocPart> execute(DocPartIndexCtx context,
      ImmutableMetaDocPart.Builder parentBuilder) throws IllegalArgumentException {
    parentBuilder.put(context.getChanged().immutableCopy());
    return ExecutionResult.success();
  }

}
