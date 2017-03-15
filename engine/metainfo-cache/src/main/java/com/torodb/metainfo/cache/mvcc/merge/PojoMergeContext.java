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

import com.torodb.core.transaction.metainf.ChangedElement;
import com.torodb.core.transaction.metainf.MetaElementState;

/**
 *
 */
public class PojoMergeContext<CommitedParentT, ChangedT>
    implements MergeContext<CommitedParentT, ChangedT> {

  private final CommitedParentT commitedParent;
  private final ChangedT changed;
  private final MetaElementState change;

  public PojoMergeContext(CommitedParentT commitedParent, ChangedT changed,
      MetaElementState change) {
    this.commitedParent = commitedParent;
    this.changed = changed;
    this.change = change;
  }

  public PojoMergeContext(CommitedParentT commitedParent, 
      ChangedElement<? extends ChangedT> changed) {
    this(commitedParent, changed.getElement(), changed.getChange());
  }

  @Override
  public CommitedParentT getCommitedParent() {
    return commitedParent;
  }

  @Override
  public ChangedT getChanged() {
    return changed;
  }

  @Override
  public MetaElementState getChange() {
    return change;
  }

}
