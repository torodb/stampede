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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.torodb.torod.core.Session;
import com.torodb.torod.core.annotations.DatabaseName;
import com.torodb.torod.core.cursors.CursorId;
import com.torodb.torod.core.dbWrapper.DbWrapper;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.executor.SessionExecutor;
import com.torodb.torod.core.executor.SessionTransaction;
import com.torodb.torod.core.executor.ToroTaskExecutionException;
import com.torodb.torod.core.language.projection.Projection;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.pojos.CollectionMetainfo;
import com.torodb.torod.core.subdocument.SplitDocument;
import com.torodb.torod.db.backends.executor.report.ReportFactory;
import com.torodb.torod.db.backends.executor.jobs.*;

import java.util.List;
import java.util.concurrent.Callable;
import javax.inject.Inject;

/**
 *
 */
class DefaultSessionExecutor implements SessionExecutor {

    private final LazyDbWrapper wrapper;
    private final ListeningExecutorService executorService;
    private final Monitor monitor;
    private final ExecutorServiceProvider executorServiceProvider;
    private final ExceptionHandler exceptionHandler;
    private final Session session;
    private final ReportFactory reportFactory;
    private final String databaseName;
    
    @Inject
    public DefaultSessionExecutor(
            ExceptionHandler exceptionHandler,
            DbWrapper wrapper, 
            ExecutorServiceProvider executorServiceProvider,
            Monitor monitor,
            Session session,
            ReportFactory reportFactory,
            @DatabaseName String databaseName) {
        this.executorServiceProvider = executorServiceProvider;
        this.exceptionHandler = exceptionHandler;
        this.wrapper = new LazyDbWrapper(wrapper);
        this.executorService = executorServiceProvider.consumeSessionExecutorService(session);
        this.monitor = monitor;
        this.session = session;
        this.reportFactory = reportFactory;
        this.databaseName = databaseName;
    }

    @Override
    public void pauseUntil(final long tick) {
        executorService.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    monitor.awaitFor(tick);
                } catch (InterruptedException ex) {
                    //TODO: Study exceptions
                    throw new RuntimeException(ex);
                }
            }
        });
    }
    
    protected <R> ListenableFuture<R> submit(Job<R> job) {
        return executorService.submit(new DefaultSessionExecutor.SessionRunnable(job));
    }

    @Override
    public SessionTransaction createTransaction() throws ImplementationDbException {
        return new DefaultSessionTransaction(
                this, 
                wrapper.consumeSessionDbConnection(), 
                reportFactory, 
                databaseName
        );
    }

    @Override
    public ListenableFuture<Void> query(
            String collection, 
            CursorId cursorId,
            QueryCriteria filter,
            Projection projection,
            int maxResults) {
        return submit(
                new QueryCallable(
                        wrapper,
                        reportFactory.createQueryReport(), 
                        collection, 
                        cursorId,
                        filter,
                        projection,
                        maxResults
                )
        );
    }

    @Override
    public ListenableFuture<List<CollectionMetainfo>> getCollectionsMetainfo() {
        return submit(
                new GetCollectionsMetainfoCallable(
                        wrapper,
                        reportFactory.createGetCollectionsMetainfoReport()
                )
        );
    }

    @Override
    public ListenableFuture<List<? extends SplitDocument>> readCursor(CursorId cursorId, int limit)
            throws ToroTaskExecutionException {
        return submit(
                new ReadCursorCallable(
                        wrapper,
                        reportFactory.createReadCursorReport(), 
                        cursorId, 
                        limit
                )
        );
    }

    @Override
    public ListenableFuture<List<? extends SplitDocument>> readAllCursor(CursorId cursorId)
            throws ToroTaskExecutionException {
        return submit(
                new ReadAllCursorCallable(
                        wrapper,
                        reportFactory.createReadAllCursorReport(), 
                        cursorId
                )
        );
    }

    @Override
    public ListenableFuture<?> closeCursor(CursorId cursorId) throws
            ToroTaskExecutionException {
        return submit(
                new CloseCursorCallable(
                        wrapper,
                        reportFactory.createCloseCursorReport(), 
                        cursorId
                )
        );
    }

    @Override
    public ListenableFuture<Integer> getMaxElements(CursorId cursorId) {
        return submit(
                new MaxElementsCallable(wrapper, reportFactory.createMaxElementsReport(), cursorId)
        );
    }

    @Override
    public ListenableFuture<Void> noop() {
        return submit(new NoopJob());
    }
    
    @Override
    public void close() {
        executorServiceProvider.releaseExecutorService(executorService);
    }

    private class SessionRunnable<R> implements Callable<R> {

        private final Job<R> delegate;

        public SessionRunnable(Job<R> delegate) {
            this.delegate = delegate;
        }

        @Override
        public R call() throws Exception {
            try {
                return delegate.call();
            } catch (Throwable ex) {
                return exceptionHandler.catchSessionException(ex, this, session);
            }
        }
    }
}
