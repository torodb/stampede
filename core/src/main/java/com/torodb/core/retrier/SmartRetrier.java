/*
 * MongoWP - ToroDB-poc: Core
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.torodb.core.retrier;

import com.torodb.common.util.RetryHelper;
import com.torodb.common.util.RetryHelper.ExceptionHandler;
import com.torodb.common.util.RetryHelper.IncrementalWaitExceptionHandler;
import com.torodb.common.util.RetryHelper.RetryCallback;
import java.util.EnumSet;
import java.util.function.IntBinaryOperator;
import java.util.function.IntPredicate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
public class SmartRetrier extends AbstractHintableRetrier {

    private static final Logger LOGGER = LogManager.getLogger(SmartRetrier.class);
    private final IntPredicate criticalGiveUpPredicate;
    private final IntPredicate infrequentGiveUpPredicate;
    private final IntPredicate frequentGiveUpPredicate;
    private final IntPredicate defaultGiveUpPredicate;
    private final MillisToWaitFunction millisToWaitFunction;
    
    public SmartRetrier(IntPredicate criticalGiveUpPredicate, IntPredicate infrequentGiveUpPredicate,
            IntPredicate frequentGiveUpPredicate, IntPredicate defaultGiveUpPredicate,
            MillisToWaitFunction millisToWaitFunction) {
        this.infrequentGiveUpPredicate = infrequentGiveUpPredicate;
        this.frequentGiveUpPredicate = frequentGiveUpPredicate;
        this.defaultGiveUpPredicate = defaultGiveUpPredicate;
        this.criticalGiveUpPredicate = criticalGiveUpPredicate;
        this.millisToWaitFunction = millisToWaitFunction;
    }

    @Override
    protected <Result, T extends Exception> ExceptionHandler<Result, T> getExceptionHandler(
            EnumSet<Hint> hints, ExceptionHandler<Result, T> delegateHandler) {

        IntPredicate giveUpPredicate = getGiveUpPredicate(hints);

        if (hints.contains(Hint.TIME_SENSIBLE)) {
            return createWithTimeHandler(giveUpPredicate, delegateHandler);
        }
        else {
            return createWithoutTimeHandler(giveUpPredicate, delegateHandler);
        }

    }

    private IntPredicate getGiveUpPredicate(EnumSet<Hint> hints) {
        if (hints.contains(Hint.CRITICAL)) {
            return criticalGiveUpPredicate;
        }
        if (hints.contains(Hint.INFREQUENT_ROLLBACK)) {
            return infrequentGiveUpPredicate;
        }
        if (hints.contains(Hint.FREQUENT_ROLLBACK)) {
            return frequentGiveUpPredicate;
        }
        return defaultGiveUpPredicate;
    }

    /**
     *
     * @param millis
     * @param attempts
     * @param giveUpPredicate
     * @return A value that follows the semantic of
     *         {@link RetryHelper#IncrementalWaitExceptionHandler} int binary operator, meaning that
     *         a zero or positive value indicates the number of millis to wait and a negative one
     *         indicates that the handler should give up
     */
    private int getMillisToWait(int attempts, int millis, IntPredicate giveUpPredicate) {
        if (giveUpPredicate.test(attempts)) {
            LOGGER.debug("Giving up when executing a task after {} executions (last execution took {} millis)", attempts, millis);
            return -1;
        }
        int millisToWait = millisToWaitFunction.applyAsInt(attempts, millis);
        if (LOGGER.isTraceEnabled()) {
            int newAttempt = attempts + 1;
            if (millisToWait == 0) {
                LOGGER.trace("Trying to execute a task for {}th time (last "
                        + "execution took {} millis)", newAttempt, millis);
            } else if (millisToWait > 0) {
                LOGGER.trace("Sleeping {} millis before trying to execute a "
                        + "task for {}th time", millisToWait, newAttempt);
            } else {
                assert millisToWait < 0;
                LOGGER.debug("Giving up when executing a task after {} "
                        + "executions (last execution took {} millis)", attempts, millis);
            }
        }

        return millisToWait;
    }

    private <Result, T extends Exception> ExceptionHandler<Result, T> createWithoutTimeHandler(
            IntPredicate giveUpPredicate, ExceptionHandler<Result, T> delegateHandler) {
        return (RetryCallback<Result> callback, Exception t, int attempts) -> {
            if (giveUpPredicate.test(attempts)) {
                LOGGER.debug("Giving up when executing a task after {} executions", attempts, t);
                delegateHandler.handleException(callback, t, attempts);
            }
            else {
                LOGGER.trace("Trying to execute a task for {}th time", attempts);
                callback.doRetry();
            }
        };
    }

    private <Result, T extends Exception> ExceptionHandler<Result, T> createWithTimeHandler(
            IntPredicate giveUpPredicate, ExceptionHandler<Result, T> delegateHandler) {
        return new IncrementalWaitExceptionHandler<>(
                (millis, attempts) -> getMillisToWait(attempts, millis, giveUpPredicate),
                delegateHandler
        );
    }

    @FunctionalInterface
    public static interface MillisToWaitFunction extends IntBinaryOperator{

        /**
         *
         * @param attempts the number of times this task was executed and failed
         * @param millis   the number of milliseconds that had been waited on
         *                 the previous iteration
         * @return the number of milliseconds that should be waited before the
         *         next iteration. If the number is negative, the executor gives
         *         up
         */
        @Override
        public int applyAsInt(int attempts, int millis);
    }
}
