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

import com.torodb.metainfo.cache.mvcc.merge.ExecutionResult.InnerErrorMessageFactory;
import com.torodb.metainfo.cache.mvcc.merge.ExecutionResult.ParentDescriptionFun;

import java.util.stream.Stream;

/**
 * @param <P> The commited parent type
 * @param <C> The changed self type
 * @param <PBT> The parent builder type
 * @param <CtxT> The contex type
 * @param <CBT> The self builder type
 * @param <IST> The self immutable type
 */
public abstract class ChildrenMergePartialStrategy<P, C, PBT, CtxT extends MergeContext<P, C>, 
    CBT, IST> implements PartialMergeStrategy<P, C, PBT, CtxT> {

  @Override
  public abstract boolean appliesTo(CtxT context);

  @Override
  public ExecutionResult<P> execute(CtxT context, PBT parentBuilder) throws
      IllegalArgumentException {
    CBT builder = createSelfBuilder(context);

    Stream<ExecutionResult<IST>> recursiveResultStream = streamChildResults(context, builder);

    ExecutionResult<P> result = mergeChildResults(context, recursiveResultStream);

    if (result.isSuccess()) {
      changeParent(parentBuilder, builder);
    }

    return result;
  }

  protected abstract CBT createSelfBuilder(CtxT context);

  protected abstract Stream<ExecutionResult<IST>> streamChildResults(CtxT context, CBT selfBuilder);

  protected abstract void changeParent(PBT parentBuilder, CBT selfBuilder);

  protected abstract String describeChanged(ParentDescriptionFun<P> parentDescFun, P parent,
      IST immutableSelf);

  /**
   * Given a context and a stream with the result of the sub structures, returns a single
   * {@link ExecutionResult} that describes the first error it found or a
   * {@link ExecutionResult#success() success} if there is no error.
   */
  private <S> ExecutionResult<P> mergeChildResults(CtxT context,
      Stream<ExecutionResult<IST>> childResults) {

    P parent = context.getCommitedParent();

    return childResults.filter(result -> !result.isSuccess())
        .map(result -> result.map(docPartErrorFactory -> createErrorMsgFactory(
            docPartErrorFactory,
            parent)
        ))
        .findAny()
        .orElse(ExecutionResult.success());
  }

  protected InnerErrorMessageFactory<P> createErrorMsgFactory(
      InnerErrorMessageFactory<IST> docPartErrorFactory, P parent) {

    return (parentDescFun) -> {
      ParentDescriptionFun<IST> selfDescFun = (self) -> describeChanged(
          parentDescFun,
          parent,
          self
      );
      return docPartErrorFactory.apply(selfDescFun);
    };
  }

}
