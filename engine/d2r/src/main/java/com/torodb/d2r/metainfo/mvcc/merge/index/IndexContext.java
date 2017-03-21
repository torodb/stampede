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

package com.torodb.d2r.metainfo.mvcc.merge.index;

import com.torodb.core.transaction.metainf.ChangedElement;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.MetaElementState;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaIndex;
import com.torodb.d2r.metainfo.mvcc.merge.ExtendedMergeContext;

/**
 *
 */
public class IndexContext
    extends ExtendedMergeContext<ImmutableMetaCollection, MutableMetaIndex, MutableMetaCollection> {

  public IndexContext(ImmutableMetaCollection commitedParent, MutableMetaIndex changed,
      MetaElementState change, MutableMetaCollection uncommitedParent) {
    super(commitedParent, changed, change, uncommitedParent);
  }

  public IndexContext(ImmutableMetaCollection commitedParent,
      ChangedElement<MutableMetaIndex> changed, MutableMetaCollection uncommitedParent) {
    super(commitedParent, changed, uncommitedParent);
  }

}
