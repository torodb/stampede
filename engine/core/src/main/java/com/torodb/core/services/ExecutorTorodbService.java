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

package com.torodb.core.services;

import com.google.common.util.concurrent.AbstractIdleService;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

public class ExecutorTorodbService<ExecutorServiceT extends ExecutorService>
    extends AbstractIdleService {

  private final ThreadFactory threadFactory;
  private ExecutorServiceT executorService;
  private final Function<ThreadFactory, ExecutorServiceT> executorServiceProvider;

  /**
   *
   * @param threadFactory
   * @param executorServiceProvider
   */
  public ExecutorTorodbService(ThreadFactory threadFactory,
      Function<ThreadFactory, ExecutorServiceT> executorServiceProvider) {
    this.threadFactory = threadFactory;
    this.executorServiceProvider = executorServiceProvider;
  }

  /**
   *
   * @param threadFactory
   * @param executorServiceSupplier
   */
  public ExecutorTorodbService(ThreadFactory threadFactory,
      Supplier<ExecutorServiceT> executorServiceSupplier) {
    this.threadFactory = threadFactory;
    this.executorServiceProvider = (tf) -> executorServiceSupplier.get();
  }

  @Override
  protected Executor executor() {
    return (Runnable command) -> {
      Thread thread = threadFactory.newThread(command);
      thread.start();
    };
  }

  @Override
  protected void startUp() throws Exception {
    executorService = executorServiceProvider.apply(threadFactory);
  }

  @Override
  protected void shutDown() throws Exception {
    if (executorService != null) {
      long timeout = 10;
      TimeUnit timeUnit = TimeUnit.SECONDS;
      Duration waitingDuration = Duration.ZERO;
      executorService.shutdown();

      while (!executorService.awaitTermination(timeout, timeUnit)) {
        waitingDuration = waitingDuration.plusSeconds(
            timeout * timeUnit.toSeconds(1));

        if (ignoreTermination(waitingDuration)) {
          break;
        }
      }
    }
  }

  protected boolean ignoreTermination(Duration waitingDuration) {
    return false;
  }

  protected ExecutorServiceT getExecutorService() {
    return executorService;
  }

  protected CompletableFuture<Void> execute(Runnable runnable) {
    return CompletableFuture.runAsync(runnable, executorService);
  }

  protected <O> CompletableFuture<O> execute(Supplier<O> supplier) {
    return CompletableFuture.supplyAsync(supplier, executorService);
  }

  protected <I, O> CompletableFuture<O> thenApply(
      CompletableFuture<I> previous, Function<I, O> fun) {
    return previous.thenApplyAsync(fun, executorService);
  }

}
