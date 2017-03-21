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

package com.torodb.d2r.metainfo.mvcc.merge.field;

import com.torodb.core.TableRef;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.MetaIndex;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import com.torodb.d2r.metainfo.mvcc.merge.result.ExecutionResult;
import com.torodb.d2r.metainfo.mvcc.merge.result.ParentDescriptionFun;
import org.jooq.lambda.Seq;

import java.util.Optional;

/**
 *
 */
public class MissingIndexStrategy implements FieldPartialStrategy {

  @Override
  public boolean appliesTo(FieldContext context) {
    return getCommitedById(context) == null
        && getCommitedByNameAndType(context) == null
        && getAnyMissedIndex(context).isPresent();
  }

  @Override
  public ExecutionResult<ImmutableMetaDocPart> execute(FieldContext context,
      ImmutableMetaDocPart.Builder parentBuilder) throws IllegalArgumentException {

    return ExecutionResult.error(
        getClass(),
        parentDescFun -> getErrorMessage(parentDescFun, context)
    );

  }

  public static Optional<? extends MetaIndex> getAnyMissedIndex(FieldContext ctx) {
    return ctx.getCommitedCollection().streamContainedMetaIndexes()
        .filter(oldIndex -> isMissedIndex(ctx, oldIndex))
        .findAny();
  }

  private String getErrorMessage(
      ParentDescriptionFun<ImmutableMetaDocPart> parentDescFun,
      FieldContext context) {
    String missed = getAnyMissedIndex(context)
        .map(index -> index.getName())
        .orElse("unknown");

    String parentDesc = parentDescFun.apply(context.getCommitedParent());

    return "The new field " + parentDesc + '.' + context.getChanged().getName() + " cannot be "
        + "commited as it should be indexed by index " + missed + " but it is not";

  }

  private static boolean isMissedIndex(FieldContext ctx, MetaIndex oldIndex) {

    TableRef tableRef = ctx.getCommitedParent().getTableRef();
    String fieldName = ctx.getChanged().getName();
    if (oldIndex.getMetaIndexFieldByTableRefAndName(tableRef, fieldName) == null) {
      //If the old index didn't have the new field we don't care about this index
      return false;
    }

    if (ctx.getUncommitedCollection().getMetaIndexByName(oldIndex.getName()) == null) {
      //if the current collection does not contains an index with the same name as the old index,
      //then the index is missing
      return true;
    }

    MutableMetaDocPart newStructure = ctx.getUncommitedParent();

    boolean anyMatch = Seq.seq(oldIndex.iteratorMetaDocPartIndexesIdentifiers(newStructure))
        .filter(identifiers -> identifiers.contains(ctx.getChanged().getIdentifier()))
        .anyMatch(identifiers -> newStructure.streamIndexes()
            .noneMatch(newDocPartIndex -> oldIndex.isMatch(newStructure, identifiers,
                newDocPartIndex)));

    return anyMatch;

  }

}
