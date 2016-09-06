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

package com.torodb.core.concurrent;

import java.util.concurrent.ExecutorService;

/**
 * A factory that creates {@link StreamExecutor stream executors} and custom executor services.
 */
public interface ConcurrentToolsFactory {
    
    public int getDefaultMaxThreads();

    public StreamExecutor createStreamExecutor(ExecutorService executor, int maxThreads);

    public StreamExecutor createStreamExecutor(String prefix, boolean blockerTasks, int maxThreads);

    public default StreamExecutor createStreamExecutor(ExecutorService executor) {
        return ConcurrentToolsFactory.this.createStreamExecutor(executor, getDefaultMaxThreads());
    }

    public default StreamExecutor createStreamExecutor(String prefix, boolean blockerTasks) {
        return ConcurrentToolsFactory.this.createStreamExecutor(prefix, blockerTasks, getDefaultMaxThreads());
    }

    public ExecutorService createExecutorService(String prefix, boolean blockerTasks, int maxThreads);

    public default ExecutorService createExecutorService(String prefix, boolean blockerTasks) {
        return createExecutorService(prefix, blockerTasks, getDefaultMaxThreads());
    }

}
