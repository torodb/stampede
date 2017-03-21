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

package com.torodb.d2r.metainfo.mvcc.merge.docpart;

import com.google.common.collect.Streams;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart.Builder;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaElementState;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import com.torodb.d2r.metainfo.mvcc.merge.ChildrenMergePartialStrategy;
import com.torodb.d2r.metainfo.mvcc.merge.docpartindex.DocPartIndexCtx;
import com.torodb.d2r.metainfo.mvcc.merge.docpartindex.DocPartIndexMergeStrategy;
import com.torodb.d2r.metainfo.mvcc.merge.field.FieldContext;
import com.torodb.d2r.metainfo.mvcc.merge.field.FieldMergeStrategy;
import com.torodb.d2r.metainfo.mvcc.merge.result.ExecutionResult;
import com.torodb.d2r.metainfo.mvcc.merge.result.ParentDescriptionFun;
import com.torodb.d2r.metainfo.mvcc.merge.scalar.ScalarContext;
import com.torodb.d2r.metainfo.mvcc.merge.scalar.ScalarMergeStrategy;

import java.util.stream.Stream;


/**
 * The strategy that iterates on children elements (fieldds, scalar and field indexes).
 */
public class DocPartChildrenStrategy
    extends ChildrenMergePartialStrategy<ImmutableMetaCollection, MutableMetaDocPart,
    ImmutableMetaCollection.Builder, DocPartCtx, Builder, ImmutableMetaDocPart>
    implements DocPartPartialStrategy {

  private final ScalarMergeStrategy scalarStrategy = new ScalarMergeStrategy();
  private final FieldMergeStrategy fieldStrategy = new FieldMergeStrategy();
  private final DocPartIndexMergeStrategy indexStrategy = new DocPartIndexMergeStrategy();

  @Override
  public boolean appliesTo(DocPartCtx context) {
    switch (context.getChange()) {
      case ADDED:
      case MODIFIED:
        break;
      default:
        return false;
    }

    MetaDocPart byId = getCommitedById(context);
    MetaDocPart byRef = getCommitedByTableRef(context);

    return byId != null && byId == byRef;
  }

  @Override
  protected Builder createSelfBuilder(DocPartCtx context) {
    MetaDocPart byId = getCommitedById(context);
    assert byId != null;
    return new Builder(byId.immutableCopy());
  }

  @Override
  protected Stream<ExecutionResult<ImmutableMetaDocPart>> streamChildResults(
      DocPartCtx context,
      Builder selfBuilder) {
    MutableMetaDocPart changed = context.getChanged();
    ImmutableMetaDocPart oldVersion = getCommitedById(context);
    ImmutableMetaCollection cmtParent = context.getCommitedParent();
    MutableMetaCollection uncmtParent = context.getUncommitedParent();
    return Streams.concat(
        streamScalarRecursiveErrors(oldVersion, changed, selfBuilder),
        streamFieldRecursiveErrors(oldVersion, changed, selfBuilder, cmtParent, uncmtParent),
        streamIndexRecursiveErrors(oldVersion, context, selfBuilder)
    );
  }

  @Override
  protected void changeParent(ImmutableMetaCollection.Builder parentBuilder, Builder selfBuilder) {
    parentBuilder.put(selfBuilder);
  }

  @Override
  protected String describeChanged(
      ParentDescriptionFun<ImmutableMetaCollection> parentDescFun,
      ImmutableMetaCollection parent, ImmutableMetaDocPart immutableSelf) {
    return parentDescFun.apply(parent) + '.' + immutableSelf.getIdentifier();
  }

  private Stream<ExecutionResult<ImmutableMetaDocPart>> streamScalarRecursiveErrors(
      ImmutableMetaDocPart oldVersion,
      MutableMetaDocPart changed,
      Builder newDocPartBuilder) {

    return changed.streamAddedMetaScalars()
        .map(scalar -> new ScalarContext(oldVersion, scalar, MetaElementState.ADDED))
        .map(ctx -> scalarStrategy.execute(ctx, newDocPartBuilder));
  }

  private Stream<ExecutionResult<ImmutableMetaDocPart>> streamFieldRecursiveErrors(
      ImmutableMetaDocPart oldVersion,
      MutableMetaDocPart changed,
      Builder newDocPartBuilder,
      ImmutableMetaCollection commitedParent,
      MutableMetaCollection uncommitedParent) {

    return changed.streamAddedMetaFields()
        .map(field -> new FieldContext(
            oldVersion,
            field,
            changed,
            commitedParent,
            uncommitedParent)
        )
        .map(ctx -> fieldStrategy.execute(ctx, newDocPartBuilder));
  }

  private Stream<ExecutionResult<ImmutableMetaDocPart>> streamIndexRecursiveErrors(
      ImmutableMetaDocPart oldVersion,
      DocPartCtx context,
      Builder newDocPartBuilder) {
    return context.getChanged().streamModifiedMetaDocPartIndexes()
        .map(change -> new DocPartIndexCtx(
            oldVersion, 
            change, 
            context.getChanged(), 
            context.getCommitedParent(), 
            context.getUncommitedParent())
        )
        .map(ctx -> indexStrategy.execute(ctx, newDocPartBuilder));
  }

}
