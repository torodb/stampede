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

package com.torodb.core.retrier;

import com.torodb.common.util.RetryHelper;
import com.torodb.common.util.RetryHelper.DelegateExceptionHandler;
import com.torodb.common.util.RetryHelper.ExceptionHandler;
import com.torodb.common.util.RetryHelper.RetryCallback;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;

import java.util.EnumSet;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import javax.annotation.Nullable;

/**
 *
 */
public abstract class AbstractHintableRetrier implements Retrier {

  private final ExceptionHandler<Object, RetrierGiveUpException> throwHandler =
      (RetryCallback<Object> callback, Exception t, int attempts) -> {
        throw new RetrierGiveUpException(t);
      };

  protected abstract <R, T extends Exception> ExceptionHandler<R, T> getExceptionHandler(
      EnumSet<Hint> hints, ExceptionHandler<R, T> delegateHandler);

  @SuppressWarnings("unchecked")
  protected <R> ExceptionHandler<R, RetrierGiveUpException> getExceptionHandler(
      EnumSet<Hint> hints) {
    //This horrible cast is done to not create the default handler each time
    return new AbortFailFastExceptionHandler<>(
        getExceptionHandler(
            hints,
            (ExceptionHandler<R, RetrierGiveUpException>) throwHandler
        )
    );
  }

  @Override
  public <R> R retry(Callable<R> callable, EnumSet<Hint> hints) throws RetrierGiveUpException {
    ExceptionHandler<R, RetrierGiveUpException> handler = getExceptionHandler(hints);
    return RetryHelper.retryOrThrow(handler, callable);
  }

  @Override
  public <R> R retry(Callable<R> callable, Supplier<R> defaultValueSupplier,
      EnumSet<Hint> hints) {
    ExceptionHandler<R, RetrierGiveUpException> handler = getExceptionHandler(hints);
    try {
      return RetryHelper.retryOrThrow(handler, callable);
    } catch (RetrierGiveUpException ex) {
      return defaultValueSupplier.get();
    }
  }

  @Override
  public <R, T extends Exception> R retry(Callable<R> callable,
      ExceptionHandler<R, T> handler, EnumSet<Hint> hints)
      throws T {
    ExceptionHandler<R, T> subHandler = new AbortFailFastExceptionHandler<>(
        getExceptionHandler(hints, handler)
    );
    return RetryHelper.retryOrThrow(subHandler, callable);
  }
  
  @Override
  public <R> R retryOrUserEx(Callable<R> callable, EnumSet<Hint> hints) throws
      UserException, RetrierGiveUpException {
    ExceptionHandler<R, RetrierGiveUpException> giveUpHandler = getExceptionHandler(hints);

    ExceptionHandler<R, WrapperException> throwOrUserRetrier =
        (RetryCallback<R> callback, Exception t, int attempts) -> {
          if (t instanceof UserException) {
            throw new WrapperException((UserException) t);
          }
          try {
            giveUpHandler.handleException(callback, t, attempts);
          } catch (RetrierGiveUpException ex) {
            throw new WrapperException(ex);
          }
        };

    try {
      return RetryHelper.retryOrThrow(throwOrUserRetrier, callable);
    } catch (WrapperException ex) {
      if (ex.getGiveUpException() != null) {
        throw ex.getGiveUpException();
      } else if (ex.getUserException() != null) {
        throw ex.getUserException();
      } else {
        throw new AssertionError("Unexpected case where " + WrapperException.class + " does "
            + "not wrap either a give up exception or a user exception. Its cause is "
            + ex.getCause(), ex);
      }
    }
  }

  /**
   * A {@link ExceptionHandler} that fails if {@link RetrierAbortException} is catched and delegates
   * on the given delegate in other case.
   *
   * @param <R>
   * @param <T>
   */
  private static class AbortFailFastExceptionHandler<R, T extends Exception>
      extends DelegateExceptionHandler<R, T> {

    public AbortFailFastExceptionHandler(ExceptionHandler<R, T> delegate) {
      super(delegate);
    }

    @Override
    public void handleException(RetryCallback<R> callback, Exception t, int attempts)
        throws T {
      if (t instanceof RuntimeException) {
        if (t instanceof RollbackException) {
          super.handleException(callback, t, attempts);
        } else {
          throw (RuntimeException) t;
        }
      } else {
        super.handleException(callback, t, attempts);
      }
    }

  }

  private static final class WrapperException extends Exception {

    private static final long serialVersionUID = -3680673355882631935L;
    private final UserException userException;
    private final RetrierGiveUpException giveUpException;

    public WrapperException(RetrierGiveUpException giveUpException) {
      super(giveUpException);
      this.giveUpException = giveUpException;
      this.userException = null;
    }

    public WrapperException(UserException userException) {
      super(userException);
      this.userException = userException;
      this.giveUpException = null;
    }

    @Nullable
    public UserException getUserException() {
      return userException;
    }

    @Nullable
    public RetrierGiveUpException getGiveUpException() {
      return giveUpException;
    }
  }

}
