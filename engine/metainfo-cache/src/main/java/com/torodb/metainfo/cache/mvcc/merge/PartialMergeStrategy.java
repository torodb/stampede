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

/**
 *
 */
public interface PartialMergeStrategy<P, C, PBT, CtxT extends MergeContext<P, C>>
    extends MergeStrategy<P, C, PBT, CtxT> {

  public boolean appliesTo(CtxT context);

  /**
   * {@inheritDoc }
   * @throws IllegalArgumentException if this strategy does not
   *                                  {@link #appliesTo(com.torodb.metainfo.cache.mvcc.MergeContext)
   *                                  apply to}the given context
   */
  @Override
  public ExecutionResult<P> execute(CtxT context, PBT parentBuilder)
      throws IllegalArgumentException;

}
