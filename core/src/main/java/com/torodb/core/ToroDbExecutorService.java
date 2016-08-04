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
 * along with core. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.core;

import java.util.concurrent.*;

/**
 * A special executor service that, by default, treat all runnables as blocking tasks.
 *
 * Implementations of this executor should be able to execute any (reasonable) number of tasks
 * without an infinite block (as a
 * {@link Executors#newFixedThreadPool(int) fixed thread pool executor} when active threads are
 * executing task that are waiting for others task that are queued) A {@link ForkJoinPool} whose
 * task are executed using a
 * {@link ForkJoinPool#managedBlock(java.util.concurrent.ForkJoinPool.ManagedBlocker) } or a
 * {@link Executors#newCachedThreadPool() cached thread pool executor} are good candidates.
 *
 * Some optimizations could be done when several non blocking task are being executed. To use them,
 * delegates this kind of task on the executor returned by {@link #asNonBlocking() }.
 */
public interface ToroDbExecutorService extends ExecutorService {

    @Override
    public void execute(Runnable command);

    /**
     *
     * @return a view of this executor that is optimized to execute non blocking task.
     */
    public Executor asNonBlocking();

}
