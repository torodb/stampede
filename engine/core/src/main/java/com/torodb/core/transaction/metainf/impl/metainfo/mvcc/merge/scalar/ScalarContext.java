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

package com.torodb.core.transaction.metainf.impl.metainfo.mvcc.merge.scalar;

import com.torodb.core.transaction.metainf.ChangedElement;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.MetaElementState;
import com.torodb.core.transaction.metainf.MetaScalar;
import com.torodb.core.transaction.metainf.impl.metainfo.mvcc.merge.DefaultMergeContext;

/**
 *
 */
public class ScalarContext extends DefaultMergeContext<ImmutableMetaDocPart, MetaScalar> {

  public ScalarContext(ImmutableMetaDocPart commitedParent, MetaScalar changed,
      MetaElementState change) {
    super(commitedParent, changed, change);
  }

  public ScalarContext(ImmutableMetaDocPart commitedParent,
      ChangedElement<? extends MetaScalar> changed) {
    super(commitedParent, changed);
  }

}
