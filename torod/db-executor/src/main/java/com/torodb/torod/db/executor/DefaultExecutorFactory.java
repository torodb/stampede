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

import com.torodb.torod.core.Session;
import com.torodb.torod.core.annotations.DatabaseName;
import com.torodb.torod.core.dbWrapper.DbWrapper;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.executor.ExecutorFactory;
import com.torodb.torod.core.executor.SessionExecutor;
import com.torodb.torod.core.executor.SystemExecutor;
import com.torodb.torod.db.executor.report.ReportFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 */
@ThreadSafe
public class DefaultExecutorFactory implements ExecutorFactory {

    private final AtomicBoolean initialized;
    private final DefaultSystemExecutor systemExecutor;
    private final DbWrapper dbWrapper;
    private final ExecutorServiceProvider executorServiceProvider;
    private final Monitor monitor;
    private final DefaultExceptionHandler exceptionHandler;
    private final ReportFactory reportFactory;
    private final String databaseName;

    @Inject
    public DefaultExecutorFactory(
            ExecutorServiceProvider executorServiceProvider,
            DbWrapper dbWrapper,
            ReportFactory reportFactory,
            @DatabaseName String databaseName) {
        final long initialTick = 0;
        this.exceptionHandler = new DefaultExceptionHandler();

        this.executorServiceProvider = executorServiceProvider;
        this.dbWrapper = dbWrapper;

        this.monitor = new Monitor(initialTick, 10);

        this.initialized = new AtomicBoolean(false);
        this.systemExecutor = new DefaultSystemExecutor(
                exceptionHandler,
                dbWrapper,
                executorServiceProvider,
                monitor,
                initialTick,
                reportFactory
        );
        this.reportFactory = reportFactory;
        this.databaseName = databaseName;
    }

    @Override
    public void initialize() throws ImplementationDbException {
        dbWrapper.initialize();
        initialized.set(true);
    }

    @Override
    public void shutdown() {
        executorServiceProvider.shutdown();
    }

    @Override
    public void shutdownNow() {
        executorServiceProvider.shutdownNow();
    }

    @Override
    public SessionExecutor createSessionExecutor(Session session) {
        return new DefaultSessionExecutor(
                exceptionHandler, 
                dbWrapper, 
                executorServiceProvider, 
                monitor, 
                session,
                reportFactory,
                databaseName
        );
    }

    @Override
    public SystemExecutor getSystemExecutor() {
        assert initialized.get();

        return systemExecutor;
    }

    static class DefaultExceptionHandler implements ExceptionHandler {

        @Override
        public <R> R catchSystemException(Throwable t, Callable<R> task) throws
                Exception {
            Logger.getLogger(DefaultExceptionHandler.class.getName()).log(Level.SEVERE, null, t);
            return null;
        }

        @Override
        public <R> R catchSessionException(Throwable t, Callable<R> task, Session s)
                throws Exception {
            Logger.getLogger(DefaultExceptionHandler.class.getName()).log(Level.SEVERE, null, t);
            return null;
        }
    }
}
