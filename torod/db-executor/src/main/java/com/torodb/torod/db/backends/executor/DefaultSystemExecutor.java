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


package com.torodb.torod.db.backends.executor;

import com.torodb.torod.core.dbWrapper.DbWrapper;
import com.torodb.torod.core.executor.SystemExecutor;
import com.torodb.torod.core.executor.ToroTaskExecutionException;
import com.torodb.torod.core.subdocument.SubDocType;
import com.torodb.torod.db.backends.executor.jobs.*;
import com.torodb.torod.db.backends.executor.report.ReportFactory;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import javax.json.JsonObject;

/**
 *
 */
class DefaultSystemExecutor implements SystemExecutor {
    private final DbWrapper wrapper;
    private final ExecutorService executorService;
    private final Monitor monitor;
    private final AtomicLong taskCounter;
    private final ExceptionHandler exceptionHandler;
    private final ReportFactory reportFactory;

    public DefaultSystemExecutor(
            ExceptionHandler exceptionHandler, 
            DbWrapper wrapper, 
            ExecutorServiceProvider executorServiceProvider, 
            Monitor monitor, 
            long initialTick,
            ReportFactory reportFactory) {
        this.exceptionHandler = exceptionHandler;
        this.wrapper = wrapper;
        this.executorService = executorServiceProvider.consumeSystemExecutorService();
        this.monitor = monitor;
        this.taskCounter = new AtomicLong(initialTick);
        this.reportFactory = reportFactory;
    }

    @Override
    public long getTick() {
        return taskCounter.get();
    }

    @Override
    public Future<?> createCollection(
            String collectionName, 
            JsonObject other,
            CreateCollectionCallback callback) throws ToroTaskExecutionException {
        return submit(
                new CreateCollectionCallable(
                        wrapper,
                        collectionName, 
                        null,
                        other,
                        callback, 
                        reportFactory.createCreateCollectionReport()
                )
        );
    }

    @Override
    public Future<?> dropCollection(String collection) {
        return submit(
                new DropCollectionCallable(
                        wrapper,
                        collection,
                        reportFactory.createDropCollectionReport()
                )
        );
    }

    @Override
    public Future<?> createSubDocTable(
            String collection, 
            SubDocType type, 
            CreateSubDocTypeTableCallback callback) throws ToroTaskExecutionException {
        return submit(
                new CreateSubDocTableCallable(
                        wrapper, 
                        collection, 
                        type, 
                        callback,
                        reportFactory.createCreateSubDocTableReport()
                )
        );
    }

    @Override
    public Future<?> reserveDocIds(
            String collection, 
            int idsToReserve, 
            ReserveDocIdsCallback callback) throws ToroTaskExecutionException {
        return submit(
                new ReserveSubDocIdsCallable(
                        wrapper, 
                        collection, 
                        idsToReserve, 
                        callback,
                        reportFactory.createReserveSubDocIdsReport()
                )
        );
    }

    @Override
    public Future<Map<String, Integer>> findCollections() {
        return submit(
                new FindCollectionsCallable(
                        wrapper,
                        reportFactory.createFindCollectionsReport()
                )
        );
    }

    private <R> Future<R> submit(Job<R> job) {
        taskCounter.incrementAndGet();
        return executorService.submit(new SystemRunnable<R>(job));
    }
    
    private class SystemRunnable<R> implements Callable<R> {
        private final Job<R> delegate;

        public SystemRunnable(Job<R> delegate) {
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
