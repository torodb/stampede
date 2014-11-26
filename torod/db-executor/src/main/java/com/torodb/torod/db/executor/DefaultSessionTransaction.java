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

import com.google.common.base.Supplier;
import com.torodb.torod.core.WriteFailMode;
import com.torodb.torod.core.connection.DeleteResponse;
import com.torodb.torod.core.connection.InsertResponse;
import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.dbWrapper.DbWrapper;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.exceptions.ToroImplementationException;
import com.torodb.torod.core.executor.SessionTransaction;
import com.torodb.torod.core.executor.ToroTaskExecutionException;
import com.torodb.torod.core.language.operations.DeleteOperation;
import com.torodb.torod.core.subdocument.SplitDocument;
import com.torodb.torod.db.executor.jobs.*;
import com.torodb.torod.db.executor.report.ReportFactory;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;

/**
 *
 */
public class DefaultSessionTransaction implements SessionTransaction {

    private final DefaultSessionExecutor executor;
    private final DbConnectionProvider connectionProvider;
    private boolean closed;
    private final ReportFactory reportFactory;

    DefaultSessionTransaction(
            DefaultSessionExecutor executor,
            DbWrapper dbWrapper,
            ReportFactory reportFactory) {
        this.executor = executor;
        this.connectionProvider = new DbConnectionProvider(dbWrapper);
        this.reportFactory = reportFactory;
        closed = false;
    }

    @Override
    public Future<?> rollback() {
        return executor.submit(
                new RollbackCallable(
                        connectionProvider,
                        reportFactory.createRollbackReport()
                )
        );
    }

    @Override
    public Future<?> commit() {
        return executor.submit(
                new CommitCallable(
                        connectionProvider,
                        reportFactory.createCommitReport()
                )
        );
    }

    @Override
    public void close() {
        closed = true;
        executor.submit(
                new CloseConnectionCallable(
                        connectionProvider,
                        reportFactory.createCloseConnectionReport()
                )
        );
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public Future<InsertResponse> insertSplitDocuments(
            String collection, 
            Collection<SplitDocument> documents, 
            WriteFailMode mode)
            throws ToroTaskExecutionException {
        return executor.submit(
                new InsertSplitDocumentCallable(
                        connectionProvider,
                        collection,
                        documents,
                        mode,
                        reportFactory.createInsertReport()
                ));
    }

    @Override
    public Future<DeleteResponse> delete(
            String collection, 
            List<? extends DeleteOperation> deletes, 
            WriteFailMode mode) {
        return executor.submit(
                new DeleteCallable(
                        connectionProvider, 
                        collection, 
                        deletes, 
                        mode,
                        reportFactory.createDeleteReport()
                )
        );
    }

    @Override
    public Future<?> dropCollection(String collection) {
        return executor.submit(
                new DropCollectionCallable(
                        connectionProvider,
                        collection
                )
        );
    }

    public static class DbConnectionProvider implements Supplier<DbConnection> {

        private final DbWrapper dbWrapper;
        private DbConnection connection;

        public DbConnectionProvider(DbWrapper dbWrapper) {
            this.dbWrapper = dbWrapper;
        }

        @Override
        public DbConnection get() {
            if (connection == null) {
                try {
                    connection = dbWrapper.consumeSessionDbConnection();
                }
                catch (ImplementationDbException ex) {
                    throw new ToroImplementationException(ex);
                }
                assert connection != null;
            }
            return connection;
        }
    }

}
