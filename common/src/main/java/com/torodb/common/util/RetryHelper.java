/*
 * This file is part of ToroDB.
 *
 * ToroDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ToroDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with common. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.common.util;

import com.google.common.base.Optional;
import java.util.concurrent.Callable;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 *
 */
public class RetryHelper {

    private RetryHelper() {}

    public static <R> R retry(@Nonnull ExceptionHandler<R, RuntimeException> handler, Callable<R> job) {
        return retryOrThrow(handler, job);
    }

    public static <R, T extends Exception> R retryOrThrow(@Nonnull ExceptionHandler<R, T> handler, Callable<R> job) throws T {
        RetryCallback<R> retryCallback = null;
        int attempts = 0;
        do {
            try {
                return job.call();
            } catch (Exception ex) {
                attempts++;
                if (retryCallback == null) {
                    retryCallback = new RetryCallback<>();
                }
                handler.handleException(retryCallback, ex, attempts);
                switch (retryCallback.action) {
                    case RETURN: {
                        return retryCallback.result;
                    }
                    default:
                }
            }
        } while(true);
    }

    public static class RetryCallback<Result> {
        @Nonnull
        RetryAction action = RetryAction.RETRY;
        @Nullable
        Result result;

        public void doRetry() {
            action = RetryAction.RETRY;
        }

        public void doReturn(Result result) {
            this.result = result;
            action = RetryAction.RETURN;
        }

    }

    private static enum RetryAction {
        RETRY,
        RETURN;
    }

    public static interface ExceptionHandler<Result, T extends Exception> {
        void handleException(RetryCallback<Result> callback, Exception t, int attempts) throws T;
    }

    public static <R, T extends Exception> ExceptionHandler<R, T> throwHandler() {
        return ThrowExceptionHandler.getInstance();
    }

    public static <R, T extends Exception> ExceptionHandler<R, T> alwaysRetryHandler() {
        return AlwaysRetryExceptionHandler.getInstance();
    }

    public static <R, T extends Exception> ExceptionHandler<R, T> defaultValueHandler(R defaultResult) {
        return new DefaultValueExceptionHandler<>(defaultResult);
    }

    public static <R, T extends Exception> ExceptionHandler<R, T> retryUntilHandler(int maxAttempts, R defaultValue) {
        ExceptionHandler<R, T> beforeHandler = alwaysRetryHandler();
        ExceptionHandler<R, T> afterHandler = defaultValueHandler(defaultValue);
        return new UntilAttemptsExceptionHandler<>(maxAttempts, beforeHandler, afterHandler);
    }

    public static <R, T extends Exception> ExceptionHandler<R, T> retryUntilHandler(int maxAttempts, ExceptionHandler<R, T> afterHandler) {
        ExceptionHandler<R, T> beforeHandler = alwaysRetryHandler();
        return new UntilAttemptsExceptionHandler<>(maxAttempts, beforeHandler, afterHandler);
    }

    public static <R, T extends Exception> ExceptionHandler<R, T> waitExceptionHandler(long millis) {
        return new WaitExceptionHandler<>(millis);
    }

    public static <R, T extends Exception> StorerExceptionHandler<R, T> storerExceptionHandler(Class<T> excetionClass, ExceptionHandler<R, T> delegate) {
        return new StorerExceptionHandler<>(delegate, excetionClass);
    }

    public static class ThrowExceptionHandler<R, T extends Exception> implements ExceptionHandler<R, T> {
        private static final ThrowExceptionHandler INSTANCE = new ThrowExceptionHandler();

        @SuppressWarnings("unchecked")
        private static <R2, T2 extends Exception> ThrowExceptionHandler<R2, T2> getInstance() {
            return INSTANCE;
        }

        @Override
        public void handleException(RetryCallback<R> callback, Exception t, int attempts) throws T {
            try {
                throw (T) t;
            } catch(Exception throwable) {
                throw new RuntimeException(throwable);
            }
        }
    }

    public static class AlwaysRetryExceptionHandler<R, T extends Exception> implements ExceptionHandler<R, T> {
        private static final AlwaysRetryExceptionHandler INSTANCE = new AlwaysRetryExceptionHandler();

        @SuppressWarnings("unchecked")
        private static <R2, T2 extends Exception> AlwaysRetryExceptionHandler<R2, T2> getInstance() {
            return INSTANCE;
        }

        @Override
        public void handleException(RetryCallback<R> callback, Exception t, int attempts) throws T {
            callback.doRetry();
        }
    }

    public static class DefaultValueExceptionHandler<R, T extends Exception> implements ExceptionHandler<R, T> {

        private final R defaultValue;

        public DefaultValueExceptionHandler(R defaultValue) {
            this.defaultValue = defaultValue;
        }

        @Override
        public void handleException(RetryCallback<R> callback, Exception t, int attempts) throws T {
            callback.doReturn(defaultValue);
        }

    }

    public static class DelegateExceptionHandler<Result, T extends Exception> implements ExceptionHandler<Result, T> {
        private final ExceptionHandler<Result, T> delegate;

        public DelegateExceptionHandler(ExceptionHandler<Result, T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void handleException(RetryCallback<Result> callback, Exception t, int attempts) throws T {
            delegate.handleException(callback, t, attempts);
        }
    }

    public static class UntilAttemptsExceptionHandler<Result, T extends Exception> implements ExceptionHandler<Result, T> {
        private final int maxAttempts;
        private final ExceptionHandler<Result, T> beforeLimitDelegate;
        private final ExceptionHandler<Result, T> afterLimitDelegate;

        public UntilAttemptsExceptionHandler(int maxAttempts,
                ExceptionHandler<Result, T> beforeLimitDelegate,
                ExceptionHandler<Result, T> afterLimitDelegate) {
            this.maxAttempts = maxAttempts;
            this.beforeLimitDelegate = beforeLimitDelegate;
            this.afterLimitDelegate = afterLimitDelegate;
        }

        @Override
        public void handleException(RetryCallback<Result> callback, Exception t, int attempts) throws T {
            if (attempts < maxAttempts) {
                beforeLimitDelegate.handleException(callback, t, attempts);
            }
            else {
                afterLimitDelegate.handleException(callback, t, attempts);
            }
        }
    }

    public static class WaitExceptionHandler<Result, T extends Exception> implements ExceptionHandler<Result, T> {
        private final long millis;

        public WaitExceptionHandler(long millis) {
            this.millis = millis;
        }

        @Override
        public void handleException(RetryCallback<Result> callback, Exception t, int attempts) throws T {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException ex) {
                Thread.interrupted();
            }
            callback.doRetry();
        }
    }

    public static class StorerExceptionHandler<R, T extends Exception> extends DelegateExceptionHandler<R, T> {

        private final Class<T> exClass;
        private Optional<T> thrown;

        public StorerExceptionHandler(ExceptionHandler<R, T> delegate, Class<T> exClass) {
            super(delegate);
            this.exClass = exClass;
            thrown = Optional.absent();
        }

        @Override
        @SuppressWarnings("unchecked")
        public void handleException(RetryCallback<R> callback, Exception t, int attempts) throws T {
            if (exClass.isInstance(t)) {
                thrown = Optional.of((T) t);
            }
            else {
                super.handleException(callback, t, attempts);
            }
        }

        public Optional<T> getThrown() {
            return thrown;
        }

    }
}
