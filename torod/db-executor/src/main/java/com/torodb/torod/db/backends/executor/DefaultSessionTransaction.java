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

import java.sql.Savepoint;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.ListenableFuture;
import com.torodb.torod.core.ValueRow;
import com.torodb.torod.core.WriteFailMode;
import com.torodb.torod.core.annotations.DatabaseName;
import com.torodb.torod.core.connection.DeleteResponse;
import com.torodb.torod.core.connection.InsertResponse;
import com.torodb.torod.core.connection.TransactionMetainfo;
import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.dbWrapper.DbWrapper;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.exceptions.ToroImplementationException;
import com.torodb.torod.core.executor.SessionTransaction;
import com.torodb.torod.core.executor.ToroTaskExecutionException;
import com.torodb.torod.core.language.operations.DeleteOperation;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.pojos.Database;
import com.torodb.torod.core.pojos.IndexedAttributes;
import com.torodb.torod.core.pojos.NamedToroIndex;
import com.torodb.torod.core.subdocument.SplitDocument;
import com.torodb.torod.core.subdocument.ToroDocument;
import com.torodb.torod.core.subdocument.values.ScalarValue;
import com.torodb.torod.db.backends.executor.jobs.CloseConnectionCallable;
import com.torodb.torod.db.backends.executor.jobs.CommitCallable;
import com.torodb.torod.db.backends.executor.jobs.CountCallable;
import com.torodb.torod.db.backends.executor.jobs.CreateIndexCallable;
import com.torodb.torod.db.backends.executor.jobs.DeleteCallable;
import com.torodb.torod.db.backends.executor.jobs.ReleaseSavepointCallable;
import com.torodb.torod.db.backends.executor.jobs.DropIndexCallable;
import com.torodb.torod.db.backends.executor.jobs.GetCollectionSizeCallable;
import com.torodb.torod.db.backends.executor.jobs.GetDatabasesCallable;
import com.torodb.torod.db.backends.executor.jobs.GetDocumentsSize;
import com.torodb.torod.db.backends.executor.jobs.GetIndexSizeCallable;
import com.torodb.torod.db.backends.executor.jobs.GetIndexesCallable;
import com.torodb.torod.db.backends.executor.jobs.InsertCallable;
import com.torodb.torod.db.backends.executor.jobs.Job;
import com.torodb.torod.db.backends.executor.jobs.ReadAllCallable;
import com.torodb.torod.db.backends.executor.jobs.RollbackCallable;
import com.torodb.torod.db.backends.executor.jobs.RollbackSavepointCallable;
import com.torodb.torod.db.backends.executor.jobs.SetSavepointCallable;
import com.torodb.torod.db.backends.executor.jobs.TransactionalJob;
import com.torodb.torod.db.backends.executor.report.ReportFactory;
import com.torodb.torod.db.executor.jobs.CreatePathViewsCallable;
import com.torodb.torod.db.executor.jobs.DropPathViewsCallable;
import com.torodb.torod.db.executor.jobs.SqlSelectCallable;

/**
 *
 */
public class DefaultSessionTransaction implements SessionTransaction {

    private final DefaultSessionExecutor executor;
    private final AtomicBoolean aborted;
    private boolean closed;
    private final DbConnection dbConnection;
    private final ReportFactory reportFactory;
    private final MyAborter aborter;
    private final String databaseName;
    private final TransactionMetainfo metainfo;

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultSessionTransaction.class);

    DefaultSessionTransaction(
            DefaultSessionExecutor executor,
            DbConnection dbConnection,
            ReportFactory reportFactory,
            @DatabaseName String databaseName,
            TransactionMetainfo metainfo) {
        this.aborted = new AtomicBoolean(false);
        this.dbConnection = dbConnection;
        this.executor = executor;
        this.reportFactory = reportFactory;
        this.aborter = new MyAborter();
        this.databaseName = databaseName;
        this.metainfo = metainfo;
        closed = false;
    }

    @Override
    public ListenableFuture<?> rollback() {
        return submit(
                new RollbackCallable(
                        dbConnection,
                        aborter, 
                        reportFactory.createRollbackReport()
                )
        );
    }

    @Override
    public ListenableFuture<Savepoint> setSavepoint() {
        return submit(
                new SetSavepointCallable(
                        dbConnection,
                        aborter, 
                        reportFactory.createSetSavepointReport()
                )
        );
    }

    @Override
    public ListenableFuture<?> rollback(Savepoint savepoint) {
        return submit(
                new RollbackSavepointCallable(
                        dbConnection,
                        aborter, 
                        reportFactory.createRollbackSavepointReport(),
                        savepoint
                )
        );
    }

    @Override
    public ListenableFuture<?> releaseSavepoint(Savepoint savepoint) {
        return submit(
                new ReleaseSavepointCallable(
                        dbConnection,
                        aborter, 
                        reportFactory.createReleaseSavepointReport(),
                        savepoint
                )
        );
    }

    @Override
    public ListenableFuture<?> commit() {
        return submit(
                new CommitCallable(
                        dbConnection,
                        aborter,
                        reportFactory.createCommitReport()
                )
        );
    }

    @Override
    public void close() {
        if (!closed) {
            submit(
                    new CloseConnectionCallable(
                            dbConnection,
                            aborter,
                            reportFactory.createCloseConnectionReport()
                    )
            );
            closed = true;
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public ListenableFuture<InsertResponse> insertSplitDocuments(
            String collection, 
            Collection<SplitDocument> documents, 
            WriteFailMode mode)
            throws ToroTaskExecutionException {
        return submit(
                new InsertCallable(
                        dbConnection,
                        aborter,
                        reportFactory.createInsertReport(),
                        collection,
                        documents,
                        mode
                ));
    }

    @Override
    public ListenableFuture<DeleteResponse> delete(
            String collection, 
            List<? extends DeleteOperation> deletes, 
            WriteFailMode mode) {
        return submit(
                new DeleteCallable(
                        dbConnection, 
                        aborter,
                        reportFactory.createDeleteReport(),
                        collection, 
                        deletes, 
                        mode
                )
        );
    }

    @Override
    public ListenableFuture<NamedToroIndex> createIndex(
            String collectionName,
            String indexName, 
            IndexedAttributes attributes, 
            boolean unique, 
            boolean blocking) {
        return submit(
                new CreateIndexCallable(
                        dbConnection, 
                        aborter,
                        reportFactory.createIndexReport(),
                        collectionName,
                        indexName, 
                        attributes, 
                        unique, 
                        blocking
                )
        );
    }

    @Override
    public ListenableFuture<Boolean> dropIndex(String collection, String indexName) {
        return submit(
                new DropIndexCallable(
                        dbConnection,
                        aborter,
                        reportFactory.createDropIndexReport(),
                        collection,
                        indexName
                )
        );
    }

    @Override
    public ListenableFuture<Collection<? extends NamedToroIndex>> getIndexes(String collection) {
        return submit(
                new GetIndexesCallable(
                        dbConnection, 
                        aborter,
                        reportFactory.createGetIndexReport(),
                        collection)
        );
    }

    @Override
    public ListenableFuture<List<? extends Database>> getDatabases() {
        return submit(
                new GetDatabasesCallable(
                        dbConnection,
                        aborter,
                        reportFactory.createGetDatabasesReport(),
                        databaseName
                )
        );
    }

    @Override
    public ListenableFuture<Integer> count(String collection, QueryCriteria query) {
        return submit(
                new CountCallable(
                        dbConnection,
                        aborter,
                        reportFactory.createCountReport(), 
                        collection, 
                        query
                )
        );
    }

    @Override
    public ListenableFuture<List<ToroDocument>> readAll(String collection, QueryCriteria query) {
        return submit(
                new ReadAllCallable(
                        dbConnection,
                        aborter,
                        reportFactory.createReadAllReport(), 
                        collection, 
                        query
                )
        );
    }

    @Override
    public ListenableFuture<Long> getIndexSize(String collection, String indexName) {
        return submit(
                new GetIndexSizeCallable(
                        dbConnection, 
                        aborter, 
                        reportFactory.createGetIndexSizeReport(),
                        collection, 
                        indexName
                )
        );
    }

    @Override
    public ListenableFuture<Long> getCollectionSize(String collection) {
        return submit(
                new GetCollectionSizeCallable(
                        dbConnection, 
                        aborter, 
                        reportFactory.createGetCollectionSizeReport(), 
                        collection
                )
        );
    }

    @Override
    public ListenableFuture<Long> getDocumentsSize(String collection) {
        return submit(
                new GetDocumentsSize(
                        dbConnection,
                        aborter,
                        reportFactory.createGetDocumentsSizeReport(),
                        collection
                )
        );
    }

    @Override
    public ListenableFuture<Integer> createPathViews(String collection) {
        return submit(
              new CreatePathViewsCallable(
                      dbConnection,
                      aborter,
                      reportFactory.createCreatePathViewsReport(),
                      collection
              )
        );
    }

    @Override
    public ListenableFuture<Void> dropPathViews(String collection) {
        return submit(
              new DropPathViewsCallable(
                      dbConnection,
                      aborter,
                      reportFactory.createDropPathViewsReport(),
                      collection
              )
        );
    }

    @Override
    public ListenableFuture<Iterator<ValueRow<ScalarValue<?>>>> sqlSelect(String sqlQuery) {
        return submit(
                new SqlSelectCallable(
                        dbConnection,
                        aborter,
                        reportFactory.createSqlSelectReport(),
                        sqlQuery
                )
        );
    }

    protected <R> ListenableFuture<R> submit(Job<R> callable) {
        return executor.submit(callable);
    }

    public static class DbConnectionProvider implements Supplier<DbConnection> {

        private final DbWrapper dbWrapper;
        private DbConnection connection;
        private DbConnection.Metainfo metadata;

        public DbConnectionProvider(DbWrapper dbWrapper, DbConnection.Metainfo metadata) {
            this.dbWrapper = dbWrapper;
            this.metadata = metadata;
        }

        @Override
        public DbConnection get() {
            if (connection == null) {
                try {
                    connection = dbWrapper.consumeSessionDbConnection(metadata);
                }
                catch (ImplementationDbException ex) {
                    throw new ToroImplementationException(ex);
                }
                assert connection != null;
            }
            return connection;
        }
    }
    
    private class MyAborter implements TransactionalJob.TransactionAborter {

        @Override
        public boolean isAborted() {
            return DefaultSessionTransaction.this.aborted.get();
        }

        @Override
        public <R> void abort(Job<R> job) {
            LOGGER.debug("Transaction aborted");
            DefaultSessionTransaction.this.aborted.set(true);
        }
        
    }

}
