/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */


package com.torodb.torod.db.executor;

import com.torodb.torod.core.dbWrapper.DbWrapper;
import com.torodb.torod.core.executor.SystemExecutor;
import com.torodb.torod.core.executor.ToroTaskExecutionException;
import com.torodb.torod.core.subdocument.SubDocType;
import com.torodb.torod.db.executor.jobs.CreateCollectionCallable;
import com.torodb.torod.db.executor.jobs.CreateSubDocTableCallable;
import com.torodb.torod.db.executor.jobs.FindCollectionCallable;
import com.torodb.torod.db.executor.jobs.ReserveSubDocIdsCallable;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
class DefaultSystemExecutor implements SystemExecutor {
    private final DbWrapper wrapper;
    private final ExecutorService executorService;
    private final Monitor monitor;
    private final AtomicLong taskCounter;
    private final ExceptionHandler exceptionHandler;

    public DefaultSystemExecutor(ExceptionHandler exceptionHandler, DbWrapper wrapper, ExecutorServiceProvider executorServiceProvider, Monitor monitor, long initialTick) {
        this.exceptionHandler = exceptionHandler;
        this.wrapper = wrapper;
        this.executorService = executorServiceProvider.consumeSystemExecutorService();
        this.monitor = monitor;
        this.taskCounter = new AtomicLong(initialTick);
    }

    @Override
    public long getTick() {
        return taskCounter.get();
    }

    @Override
    public Future<?> createCollection(String collection, CreateCollectionCallback callback) throws ToroTaskExecutionException {
        return submit(new CreateCollectionCallable(collection, callback, wrapper));
    }

    @Override
    public Future<?> createSubDocTable(String collection, SubDocType type, CreateSubDocTypeTableCallback callback) throws ToroTaskExecutionException {
        return submit(new CreateSubDocTableCallable(wrapper, collection, type, callback));
    }

    @Override
    public Future<?> reserveDocIds(String collection, int idsToReserve, ReserveDocIdsCallback callback) throws ToroTaskExecutionException {
        return submit(new ReserveSubDocIdsCallable(wrapper, collection, idsToReserve, callback));
    }

    @Override
    public Future<Map<String, Integer>> findCollections() {
        return submit(new FindCollectionCallable(wrapper));
    }

    @Override
    public Future<Map<String, Integer>> getLastUsedIds() throws ToroTaskExecutionException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    private <R> Future<R> submit(Callable<R> callable) {
        taskCounter.incrementAndGet();
        return executorService.submit(new SystemRunnable<R>(callable));
    }
    
    private class SystemRunnable<R> implements Callable<R> {
        private final Callable<R> delegate;

        public SystemRunnable(Callable<R> delegate) {
            this.delegate = delegate;
        }

        @Override
        public R call() throws Exception {
            try {
                R result = delegate.call();
                monitor.tick();

                return result;
            } catch (Throwable ex) {
                return exceptionHandler.catchSystemException(ex, this);
            }
        }
    }
}
