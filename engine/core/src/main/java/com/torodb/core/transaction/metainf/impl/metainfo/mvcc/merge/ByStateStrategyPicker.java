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

package com.torodb.core.transaction.metainf.impl.metainfo.mvcc.merge;

/**
 * A {@link MergeStrategyPicker} that choose between strategies by the
 * {@link MergeContext#getChange() kind of change}.
 * @param <P> The commited parent class
 * @param <C> The changed element class
 * @param <PBT> The class that is used to create new commited parents
 * @param <CtxT> The context class
 */
public class ByStateStrategyPicker<P, C, PBT, CtxT extends MergeContext<P, C>>
    implements MergeStrategyPicker<P, C, PBT, CtxT> {

  @SuppressWarnings("checkstyle:LineLength")
  private final MergeStrategyPicker<P, C, PBT, CtxT> onAdd;
  @SuppressWarnings("checkstyle:LineLength")
  private final MergeStrategyPicker<P, C, PBT, CtxT> onModify;
  @SuppressWarnings("checkstyle:LineLength")
  private final MergeStrategyPicker<P, C, PBT, CtxT> onRemove;

  public ByStateStrategyPicker(
      MergeStrategyPicker<P, C, PBT, CtxT> onAdd,
      MergeStrategyPicker<P, C, PBT, CtxT> onModify,
      MergeStrategyPicker<P, C, PBT, CtxT> onRemove) {
    this.onAdd = onAdd;
    this.onModify = onModify;
    this.onRemove = onRemove;
  }

  @Override
  public MergeStrategy<P, C, PBT, CtxT> pick(CtxT context) {
    switch (context.getChange()) {
      default:
      case NOT_CHANGED:
      case NOT_EXISTENT:
        throw new AssertionError("A modification was expected, but the new state is "
            + context.getChanged());
      case ADDED:
        return onAdd.pick(context);
      case MODIFIED:
        return onModify.pick(context);
      case REMOVED:
        return onRemove.pick(context);
    }
  }

}
