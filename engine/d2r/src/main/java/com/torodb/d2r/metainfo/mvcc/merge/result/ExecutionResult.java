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

package com.torodb.d2r.metainfo.mvcc.merge.result;

import java.util.Optional;
import java.util.function.Function;


/**
 * The result state of an execution of a {@link MergeStrategy}.
 */
public abstract class ExecutionResult<P> {

  /**
   * Returns a successful result.
   */
  @SuppressWarnings("unchecked")
  public static <P> ExecutionResult<P> success() {
    return (ExecutionResult<P>) SuccessExecutionResult.INSTANCE;
  }

  /**
   * Returns an erroneous result with a given rule id and error factory.
   *
   * @param ruleId     the rule id, used to make it easy to identify the rule that reports the error
   * @param errFactory the error factory that can be evaluated to obtain the error message.
   */
  public static <P> ExecutionResult<P> error(
      String ruleId, InnerErrorMessageFactory<P> errFactory) {
    return new ErrorExecutionResult<>(ruleId, errFactory);
  }

  /**
   * Returns an erroneous result with a given rule class and error factory.
   *
   * @param ruleClass  the rule class, used to make it easy to identify the rule that reports the
   *                   error
   * @param errFactory the error factory that can be evaluated to obtain the error message.
   */
  public static <P> ExecutionResult<P> error(
      Class<?> ruleClass,
      InnerErrorMessageFactory<P> errFactory) {
    return error(ruleClass.getCanonicalName(), errFactory);
  }

  /**
   * Returns true iff the execution was successful.
   *
   * <p>If (and only if) this is true, {@link #getErrorMessageFactory() } returns an empty optional.
   * @see #getErrorMessageFactory()
   */
  public abstract boolean isSuccess();

  /**
   * Optionally returns a function that can be used to get the error message.
   *
   * @return an empty optional if {@link #isSuccess() } or a non empty optional on other case.
   */
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



}
