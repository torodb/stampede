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

import com.torodb.core.transaction.metainf.ChangedElement;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.MetaElementState;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import com.torodb.metainfo.cache.mvcc.merge.ExtendedMergeContext;

/**
 *
 */
public class DocPartCtx extends
    ExtendedMergeContext<ImmutableMetaCollection, MutableMetaDocPart, MutableMetaCollection> {

  public DocPartCtx(ImmutableMetaCollection commitedParent, MutableMetaDocPart changed,
      MetaElementState change, MutableMetaCollection uncommitedParent) {
    super(commitedParent, changed, change, uncommitedParent);
  }

  public DocPartCtx(ImmutableMetaCollection commitedParent,
      ChangedElement<MutableMetaDocPart> changed, MutableMetaCollection uncommitedParent) {
    super(commitedParent, changed, uncommitedParent);
  }

}
