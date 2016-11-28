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

package com.torodb.core.concurrent;

import com.google.common.util.concurrent.Service;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * A class that can be used to execute stream of tasks, returning a {@link CompletableFuture} that
 * will be done when all subtasks finish.
 */
public interface StreamExecutor extends Service {

  /**
   * Executes the given runnables, returning a future that will be done once all runnables finish.
   *
   * This execution is executed on a limited number of threads.
   *
   * @param runnables
   * @return a CompletableFuture that will be done on once all runnables finish.
   */
  public CompletableFuture<?> executeRunnables(Stream<Runnable> runnables);

  /**
   * Executes the given callables, returning a future that will be done once all callables finish.
   *
   * This execution is executed on a limited number of threads.
   *
   * @param callables
   * @return a CompletableFuture that will be done on once all callables finish.
   */
  public <I> CompletableFuture<?> execute(Stream<Callable<I>> callables);

  /**
   * Executes the given callables, returning a future that will be done once all callables finish.
   *
   * The result of the given callables is folded using the given function This execution is executed
   * on a limited number of threads.
   *
   * @param <I>
   * @param <O>
   * @param callables
   * @param zero
   * @param fun
   * @return
   */
  public <I, O> CompletableFuture<O> fold(Stream<Callable<I>> callables, O zero,
      BiFunction<O, I, O> fun);

}
