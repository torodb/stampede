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

package com.torodb.d2r.metainfo.mvcc.merge.docpartindex;

import com.torodb.core.transaction.metainf.ChangedElement;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.MetaElementState;
import com.torodb.core.transaction.metainf.MetaIdentifiedDocPartIndex;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import com.torodb.d2r.metainfo.mvcc.merge.ExtendedMergeContext;

/**
 *
 */
public class DocPartIndexCtx extends ExtendedMergeContext<ImmutableMetaDocPart,
    MetaIdentifiedDocPartIndex, MutableMetaDocPart> {

  private final ImmutableMetaCollection commitedCollection;
  private final MutableMetaCollection uncommitedCollection;

  public DocPartIndexCtx(ImmutableMetaDocPart commitedParent,
      ChangedElement<? extends MetaIdentifiedDocPartIndex> changed,
      MutableMetaDocPart uncommitedParent, ImmutableMetaCollection commitedCollection,
      MutableMetaCollection uncommitedCollection) {
    super(commitedParent, changed, uncommitedParent);
    this.commitedCollection = commitedCollection;
    this.uncommitedCollection = uncommitedCollection;
  }

  public DocPartIndexCtx(ImmutableMetaDocPart commitedParent,
      MetaIdentifiedDocPartIndex changed, MetaElementState change,
      MutableMetaDocPart uncommitedParent, ImmutableMetaCollection commitedCollection,
      MutableMetaCollection uncommitedCollection) {
    super(commitedParent, changed, change, uncommitedParent);
    this.commitedCollection = commitedCollection;
    this.uncommitedCollection = uncommitedCollection;
  }

  public ImmutableMetaCollection getCommitedCollection() {
    return commitedCollection;
  }

  public MutableMetaCollection getUncommitedCollection() {
    return uncommitedCollection;
  }


}
