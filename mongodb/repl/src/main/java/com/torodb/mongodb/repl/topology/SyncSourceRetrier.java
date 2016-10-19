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
 * along with repl. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.mongodb.repl.topology;

import com.torodb.common.util.RetryHelper.ExceptionHandler;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.retrier.Retrier;
import com.torodb.core.retrier.RetrierGiveUpException;
import com.torodb.core.retrier.SmartRetrier;
import java.util.EnumSet;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;

/**
 *
 */
@ThreadSafe
public class SyncSourceRetrier implements Retrier {

    private final SmartRetrier delegate;

    public SyncSourceRetrier() {
        delegate = new SmartRetrier(SyncSourceRetrier::criticalPredicate,
                SyncSourceRetrier::infrequentPredicate,
                SyncSourceRetrier::frequentPredicate,
                SyncSourceRetrier::defaultPredicate,
                SyncSourceRetrier::millisToWait);
    }

    private static boolean criticalPredicate(int attempts) {
        return attempts > 100;
    }

    private static boolean infrequentPredicate(int attempts) {
        return attempts > 10;
    }

    private static boolean frequentPredicate(int attempts) {
        return attempts > 100;
    }

    private static boolean defaultPredicate(int attempts) {
        return attempts > 30;
    }

    private static int millisToWait(int attempts, int millis) {
        int result = millis > 1000 ? millis : 1000;
        result *= attempts;
        if (result > 30_000) {
            result = 30_000;
        }
        return result;
    }

    @Override
    public <Result> Result retry(Callable<Result> callable, EnumSet<Hint> hints)
            throws RetrierGiveUpException {
        return delegate.retry(callable, hints);
    }

    @Override
    public <Result> Result retryOrUserEx(Callable<Result> callable, EnumSet<Hint> hints)
            throws UserException, RetrierGiveUpException {
        return delegate.retryOrUserEx(callable, hints);
    }

    @Override
    public <Result> Result retry(Callable<Result> callable, Supplier<Result> defaultValueSupplier, EnumSet<Hint> hints) {
        return delegate.retry(callable, defaultValueSupplier, hints);
    }

    @Override
    public <Result, T extends Exception> Result retry(Callable<Result> callable, ExceptionHandler<Result, T> handler, EnumSet<Hint> hints)
            throws T {
        return delegate.retry(callable, handler, hints);
    }

}
