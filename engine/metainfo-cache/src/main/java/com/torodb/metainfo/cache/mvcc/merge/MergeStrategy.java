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

package com.torodb.metainfo.cache.mvcc.merge;

import com.torodb.metainfo.cache.mvcc.merge.result.ExecutionResult;

/**
 * A rule or strategy to merge an element.
 * @param <P> the type of the commited parent
 * @param <C> the type of the changed element
 * @param <PBT> the builder class to create new parents
 * @param <CtxT> the context type
 */
public interface MergeStrategy<P, C, PBT, CtxT extends MergeContext<P, C>> {

  ExecutionResult<P> execute(CtxT context, PBT parentBuilder);

}
