
package com.torodb.core.concurrent;

import com.google.common.util.concurrent.Service;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * A class that can be used to execute stream of tasks, returning a 
 * {@link CompletableFuture} that will be done when all subtasks finish.
 */
public interface StreamExecutor extends Service {

    /**
     * Executes the given runnables, returning a future that will be done once all runnables finish.
     *
     * This execution is executed on a limited number of threads.
     * @param runnables
     * @return a CompletableFuture that will be done on once all runnables finish.
     */
    public CompletableFuture<?> executeRunnables(Stream<Runnable> runnables);

    /**
     * Executes the given callables, returning a future that will be done once all callables finish.
     *
     * This execution is executed on a limited number of threads.
     * @param callables
     * @return a CompletableFuture that will be done on once all callables finish.
     */
    public <I> CompletableFuture<?> execute(Stream<Callable<I>> callables);

    /**
     * Executes the given callables, returning a future that will be done once all callables finish.
     *
     * The result of the given callables is folded using the given function
     * This execution is executed on a limited number of threads.
     * @param <I>
     * @param <O>
     * @param callables
     * @param zero
     * @param fun
     * @return
     */
    public <I, O> CompletableFuture<O> fold(Stream<Callable<I>> callables, O zero, BiFunction<O, I, O> fun);

}
