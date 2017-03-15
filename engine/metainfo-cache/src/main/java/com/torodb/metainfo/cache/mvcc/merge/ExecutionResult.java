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

import java.util.Optional;
import java.util.function.Function;


/**
 * The result state of an execution of a {@link MergeStrategy}.
 */
public abstract class ExecutionResult<P> {

  @SuppressWarnings("unchecked")
  public static <P> ExecutionResult<P> success() {
    return (ExecutionResult<P>) SuccessExecutionResult.INSTANCE;
  }

  public static <P> ExecutionResult<P> error(
      String ruleId, InnerErrorMessageFactory<P> errFactory) {
    return new ErrorExecutionResult<>(ruleId, errFactory);
  }

  public static <P> ExecutionResult<P> error(
      Class<?> ruleClass,
      InnerErrorMessageFactory<P> errFactory) {
    return error(ruleClass.getCanonicalName(), errFactory);
  }

  public abstract boolean isSuccess();

  public abstract Optional<PrettyErrorMessageFactory<P>> getErrorMessageFactory();

  /**
   * Returns a new {@link ExecutionResult} that maps the error message to a new function.
   *
   * @param trans a function that maps from this error message to the new one.
   */
  public <P2> ExecutionResult<P2> map(
      Function<InnerErrorMessageFactory<P>, InnerErrorMessageFactory<P2>> trans) {
    if (this.isSuccess()) {
      return ExecutionResult.success();
    } else {
      return new MapErrorExecutionResult<>(this, trans);
    }
  }

  @FunctionalInterface
  public static interface InnerErrorMessageFactory<P>
      extends Function<ParentDescriptionFun<P>, String> {

    @Override
    public String apply(ParentDescriptionFun<P> parentDescriptionFun);

  }

  @FunctionalInterface
  public static interface ParentDescriptionFun<P> extends Function<P, String> {

    @Override
    public String apply(P parent);
  }

  public static class PrettyErrorMessageFactory<P>
      implements Function<ParentDescriptionFun<P>, String> {
    private final String ruleId;
    private final InnerErrorMessageFactory<P> delegate;

    public PrettyErrorMessageFactory(String ruleId, InnerErrorMessageFactory<P> delegate) {
      this.ruleId = ruleId;
      this.delegate = delegate;
    }

    @Override
    public String apply(ParentDescriptionFun<P> parentDescriptionFun) {
      return ruleId + ": " + delegate.apply(parentDescriptionFun);
    }

    public <P2> PrettyErrorMessageFactory<P2> transform(
        Function<InnerErrorMessageFactory<P>, InnerErrorMessageFactory<P2>> transformation) {
      return new PrettyErrorMessageFactory<>(ruleId, transformation.apply(delegate));
    }
  }
}
