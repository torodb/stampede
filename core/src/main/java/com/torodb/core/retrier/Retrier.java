
package com.torodb.core.retrier;

import com.torodb.common.util.RetryHelper.ExceptionHandler;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import java.util.EnumSet;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;

/**
 *
 */
@ThreadSafe
public interface Retrier {

    /**
     * Executes the callable until it finished correctly, a runtime exception
     * different than {@link RollbackException} is thrown or the replier policy
     * decides to give up.
     *
     * @param <Result>
     * @param callable
     * @param hints
     * @return the value returned by the callable on succeful executions
     * @throws RetrierGiveUpException if the policy decides to give up
     * @throws RuntimeException       if the callable thrown a runtime exception
     *                                that is not a RollbackException, that
     *                                exception is rethrown
     */
    public <Result> Result retry(Callable<Result> callable, EnumSet<Hint> hints)
            throws RetrierGiveUpException;

    /**
     * Executes the callable until it finished correctly, a runtime exception
     * different than {@link RollbackException} is thrown, a
     * {@link UserException} is thrown or the replier policy decides to give up.
     *
     * @param <Result>
     * @param callable the task to be done
     * @param hints
     * @return the value returned by the callable on succeful executions
     * @throws RetrierGiveUpException if the policy decides to give up
     * @throws RuntimeException       if the callable thrown a runtime exception
     *                                that is not a RollbackException, that
     *                                exception is rethrown
     * @throws UserException          if the callable thrown a user exception,
     *                                that exception is rethrown
     */
    public <Result> Result retryOrUserEx(Callable<Result> callable, EnumSet<Hint> hints) throws UserException,
            RetrierGiveUpException, RuntimeException;

    /**
     * Executes the callable until it finished correctly, a runtime exception
     * different than {@link RollbackException} is thrown or the replier policy
     * decides to give up, in which case returns the value returned by the given
     * supplier.
     *
     * @param <Result>
     * @param callable             the task to be done
     * @param defaultValueSupplier a supplier whose value will be returned if
     *                             the replier gives up.
     * @param hints
     * @return the value returned by the callable on succesfull executions or
     *         the value returned by the supplier if the retrier gives up
     * @throws RuntimeException if the callable thrown a runtime exception that
     *                          is not a RollbackException, that exception is
     *                          rethrown
     */
    public <Result> Result retry(Callable<Result> callable, Supplier<Result> defaultValueSupplier, EnumSet<Hint> hints) throws RuntimeException;

    /**
     * Executes the callable until it finished correctly, a runtime exception
     * different than {@link RollbackException} is thrown or the replier policy
     * decides to give up, in which case it delegates on the given
     * {@link ExceptionHandler}.
     *
     * @param <Result>
     * @param <T>      The kind of exception the given handler can throw
     * @param callable the task to be done
     * @param handler  The handler that will be used to handler checked or
     *                 rollback exceptions once the replier gives up.
     * @param hints
     * @return the value returned by the callable on succesfull executions
     * @throws T                When the replier gives up and the given handler
     *                          decides to throw it.
     * @throws RuntimeException if the callable thrown a runtime exception that
     *                          is not a RollbackException, that exception is
     *                          rethrown
     */
    public <Result, T extends Exception> Result retry(Callable<Result> callable, ExceptionHandler<Result, T> handler, EnumSet<Hint> hints)
            throws T, RuntimeException;

    /**
     * @see #retry(java.util.concurrent.Callable, java.util.EnumSet)
     */
    public default <Result> Result retry(Callable<Result> callable) throws RetrierGiveUpException, RetrierAbortException {
        return retry(callable, EnumSet.noneOf(Hint.class));
    }

    /**
     * @see #retry(java.util.concurrent.Callable, java.util.EnumSet)
     */
    public default <Result> Result retry(Callable<Result> callable, Hint hint) throws RetrierGiveUpException, RetrierAbortException {
        return retry(callable, EnumSet.of(hint));
    }

    /**
     * @see #retry(java.util.concurrent.Callable, java.util.EnumSet)
     */
    public default <Result> Result retry(Callable<Result> callable, Hint hint1, Hint hint2) throws RetrierGiveUpException, RetrierAbortException {
        return retry(callable, EnumSet.of(hint1, hint2));
    }

    /**
     * @see #retry(java.util.concurrent.Callable, java.util.EnumSet)
     */
    public default <Result> Result retry(Callable<Result> callable, Hint hint1, Hint hint2, Hint hint3) throws RetrierGiveUpException, RetrierAbortException {
        return retry(callable, EnumSet.of(hint1, hint2, hint3));
    }

    /**
     * @see #retryOrUserEx(java.util.concurrent.Callable, java.util.EnumSet)
     */
    public default <Result> Result retryOrUserEx(Callable<Result> callable) throws UserException,
            RetrierGiveUpException, RetrierAbortException {
        return retryOrUserEx(callable, EnumSet.noneOf(Hint.class));
    }

    /**
     * @see #retryOrUserEx(java.util.concurrent.Callable, java.util.EnumSet)
     */
    public default <Result> Result retryOrUserEx(Callable<Result> callable, Hint hint) throws UserException,
            RetrierGiveUpException, RetrierAbortException {
        return retryOrUserEx(callable, EnumSet.of(hint));
    }

    /**
     * @see #retryOrUserEx(java.util.concurrent.Callable, java.util.EnumSet)
     */
    public default <Result> Result retryOrUserEx(Callable<Result> callable, Hint hint1, Hint hint2) throws UserException,
            RetrierGiveUpException, RetrierAbortException {
        return retryOrUserEx(callable, EnumSet.of(hint1, hint2));
    }

    /**
     * @see #retryOrUserEx(java.util.concurrent.Callable, java.util.EnumSet)
     */
    public default <Result> Result retryOrUserEx(Callable<Result> callable, Hint hint1, Hint hint2, Hint hint3) throws UserException,
            RetrierGiveUpException, RetrierAbortException {
        return retryOrUserEx(callable, EnumSet.of(hint1, hint2, hint3));
    }

    /**
     * @see #retry(java.util.concurrent.Callable, java.util.function.Supplier, java.util.EnumSet)
     */
    public default <Result> Result retry(Callable<Result> callable, Supplier<Result> defaultValueSupplier) {
        return retry(callable, defaultValueSupplier, EnumSet.noneOf(Hint.class));
    }

    /**
     * @see #retry(java.util.concurrent.Callable, java.util.function.Supplier, java.util.EnumSet)
     */
    public default <Result> Result retry(Callable<Result> callable, Supplier<Result> defaultValueSupplier, Hint hint) {
        return retry(callable, defaultValueSupplier, EnumSet.of(hint));
    }

    /**
     * @see #retry(java.util.concurrent.Callable, java.util.function.Supplier, java.util.EnumSet)
     */
    public default <Result> Result retry(Callable<Result> callable, Supplier<Result> defaultValueSupplier, Hint hint1, Hint hint2) {
        return retry(callable, defaultValueSupplier, EnumSet.of(hint1, hint2));
    }

    /**
     * @see #retry(java.util.concurrent.Callable, java.util.function.Supplier, java.util.EnumSet)
     */
    public default <Result> Result retry(Callable<Result> callable, Supplier<Result> defaultValueSupplier, Hint hint1, Hint hint2, Hint hint3) {
        return retry(callable, defaultValueSupplier, EnumSet.of(hint1, hint2, hint3));
    }

    /**
     * @see #retry(java.util.concurrent.Callable, com.torodb.common.util.RetryHelper.ExceptionHandler, java.util.EnumSet)
     */
    public default <Result, T extends Exception> Result retry(Callable<Result> callable, ExceptionHandler<Result, T> handler) throws T {
        return retry(callable, handler, EnumSet.noneOf(Hint.class));
    }

    /**
     * @see #retry(java.util.concurrent.Callable, com.torodb.common.util.RetryHelper.ExceptionHandler, java.util.EnumSet)
     */
    public default <Result, T extends Exception> Result retry(Callable<Result> callable, ExceptionHandler<Result, T> handler, Hint hint) throws T {
        return retry(callable, handler, EnumSet.of(hint));
    }

    /**
     * @see #retry(java.util.concurrent.Callable, com.torodb.common.util.RetryHelper.ExceptionHandler, java.util.EnumSet)
     */
    public default <Result, T extends Exception> Result retry(Callable<Result> callable, ExceptionHandler<Result, T> handler, Hint hint1, Hint hint2) throws T {
        return retry(callable, handler, EnumSet.of(hint1, hint2));
    }

    /**
     * @see #retry(java.util.concurrent.Callable, com.torodb.common.util.RetryHelper.ExceptionHandler, java.util.EnumSet)
     */
    public default <Result, T extends Exception> Result retry(Callable<Result> callable, ExceptionHandler<Result, T> handler, Hint hint1, Hint hint2, Hint hint3) throws T {
        return retry(callable, handler, EnumSet.of(hint1, hint2, hint3));
    }

    public static enum Hint {

        /**
         * For task that it is critical to be executed.
         * <p>
         * This hint indicates that the retrier should try to execute the task more times before it
         * gives up when rollbacks are recived, because once it does, ToroDB could become unestable
         * or at least it will be difficult or expensive to recover. The retrier could even decide
         * to never give up when this hint is sent.
         */
        CRITICAL,
        /**
         * For task that usually recives rollbacks.
         * <p>
         * This hint indicates that the retrier should try to execute the task more times before it
         * gives up when rollbacks are recived.
         */
        FREQUENT_ROLLBACK,
        /**
         * For task that do not usually recives rollbacks.
         * <p>
         * This hint indicates that the retrier should try to execute the task less times before it
         * gives up when rollbacks are recived.
         */
        INFREQUENT_ROLLBACK,
        /**
         * For task whose rollbacks can be usually be reduced if the next attempt is done after
         * waiting a short amount of time.
         */
        TIME_SENSIBLE
    }
}
