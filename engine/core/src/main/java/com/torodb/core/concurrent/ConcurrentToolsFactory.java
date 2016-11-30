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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A factory that creates {@link StreamExecutor stream executors} and custom executor services.
 */
public interface ConcurrentToolsFactory {

  public int getDefaultMaxThreads();

  public StreamExecutor createStreamExecutor(String prefix, boolean blockerTasks, int maxThreads);

  public default StreamExecutor createStreamExecutor(String prefix, boolean blockerTasks) {
    return ConcurrentToolsFactory.this.createStreamExecutor(prefix, blockerTasks,
        getDefaultMaxThreads());
  }

  /**
   * Creates an {@link ScheduledExecutorService} with the given number of max threads.
   *
   * @param prefix
   * @param maxThreads
   * @return
   */
  public ScheduledExecutorService createScheduledExecutorServiceWithMaxThreads(String prefix,
      int maxThreads);

  /**
   * Creates an executor service with the given number of max threads.
   *
   * @param prefix
   * @param maxThreads
   * @return
   */
  public ExecutorService createExecutorServiceWithMaxThreads(String prefix, int maxThreads);

  /**
   * Creates an executor service with the a parallelism number of threads.
   *
   * @param prefix
   * @param blockerTasks if executed task can block or not
   * @param parallelism  the aproximated number of threads that the executor service should have. It
   *                     may be treated as a min number of threads or as {@link ForkJoinPool} treat
   *                     parallelism.
   * @return
   */
  public ExecutorService createExecutorService(String prefix, boolean blockerTasks,
      int parallelism);

  public default ExecutorService createExecutorService(String prefix, boolean blockerTasks) {
    return createExecutorService(prefix, blockerTasks, getDefaultMaxThreads());
  }

}
