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

package com.toro.torod.connection;

import com.google.common.collect.FluentIterable;
import com.torodb.torod.core.Session;
import com.torodb.torod.core.config.DocumentBuilderFactory;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.core.connection.ToroTransaction;
import com.torodb.torod.core.connection.TransactionMetainfo;
import com.torodb.torod.core.cursors.CursorId;
import com.torodb.torod.core.cursors.ToroCursor;
import com.torodb.torod.core.cursors.ToroCursorManager;
import com.torodb.torod.core.cursors.UserCursor;
import com.torodb.torod.core.d2r.D2RTranslator;
import com.torodb.torod.core.dbMetaInf.DbMetaInformationCache;
import com.torodb.torod.core.dbWrapper.DbWrapper;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.exceptions.*;
import com.torodb.torod.core.executor.ExecutorFactory;
import com.torodb.torod.core.executor.SessionExecutor;
import com.torodb.torod.core.executor.SessionTransaction;
import com.torodb.torod.core.language.projection.Projection;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.pojos.CollectionMetainfo;
import com.torodb.torod.core.subdocument.ToroDocument;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import javax.annotation.concurrent.NotThreadSafe;
import javax.json.JsonObject;

import static java.lang.Thread.currentThread;

/**
 *
 */
@NotThreadSafe
class DefaultToroConnection implements ToroConnection {

    private final Session session;
    private final D2RTranslator d2r;
    private final SessionExecutor executor;
    private final DocumentBuilderFactory documentBuilderFactory;
    private final DbMetaInformationCache cache;
    private final ToroCursorManager cursorManager;

    DefaultToroConnection(
            ToroCursorManager cursorManager,
            D2RTranslator d2RTranslator,
            ExecutorFactory executorFactory,
            DbWrapper dbWrapper,
            DocumentBuilderFactory documentBuilderFactory,
            DbMetaInformationCache cache) {
        this.session = new DefaultSession();
        this.d2r = d2RTranslator;

        this.executor = executorFactory.createSessionExecutor(session);
        this.documentBuilderFactory = documentBuilderFactory;
        this.cache = cache;
        this.cursorManager = cursorManager;
    }

    @Override
    public UserCursor getCursor(CursorId cursorId) throws CursorNotFoundException {
        return new MyUserCursor(cursorManager.lookForCursor(cursorId));
    }

    @Override
    public UserCursor openUnlimitedCursor(
            String collection, 
            QueryCriteria queryCriteria, 
            Projection projection, 
            int numberToSkip, 
            boolean autoclose, 
            boolean hasTimeout) throws NotAutoclosableCursorException {
        return new MyUserCursor(
                cursorManager.openUnlimitedCursor(
                        executor,
                        collection, 
                        queryCriteria, 
                        projection, 
                        numberToSkip, 
                        autoclose, 
                        hasTimeout
                )
        );
    }

    @Override
    public UserCursor openLimitedCursor(
            String collection, 
            QueryCriteria queryCriteria, 
            Projection projection, 
            int numberToSkip, 
            int limit, 
            boolean autoclose, 
            boolean hasTimeout) throws ToroException {
        return new MyUserCursor(
                cursorManager.openLimitedCursor(
                        executor,
                        collection, 
                        queryCriteria, 
                        projection, 
                        numberToSkip, 
                        limit, 
                        autoclose, 
                        hasTimeout
                )
        );
    }

    @Override
    public FluentIterable<CollectionMetainfo> getCollectionsMetainfoCursor() throws ToroException {
        try {
            return executor.getCollectionsMetainfo().get();
        } catch (ExecutionException ex) {
            if (ex.getCause() != null) {
                if (ex.getCause() instanceof ToroException) {
                    throw new ToroException(ex.getCause());
                }
            }
            throw new ToroRuntimeException(ex);
        } catch (InterruptedException ex) {
            currentThread().interrupt();
            throw new ToroRuntimeException(ex);
        }
    }

    @Override
    public Session getSession() {
        return session;
    }

    @Override
    public Collection<String> getCollections() {
        return cache.getCollections();
    }

    @Override
    public boolean createCollection(String collection, JsonObject otherInfo) {
        if(cache.collectionExists(collection)) {
            return false;
        }

        return cache.createCollection(executor, collection, otherInfo);
    }
    
    @Override
    public boolean dropCollection(String collection) {
        return cache.dropCollection(executor, collection);
    }

    @Override
    public void close() {
        executor.close();
    }

    @Override
    public ToroTransaction createTransaction(TransactionMetainfo metainfo) throws ImplementationDbException {
        SessionTransaction sessionTransaction = executor.createTransaction(metainfo);
        return new DefaultToroTransaction(
                cache,
                this, 
                sessionTransaction, 
                d2r, 
                executor, 
                documentBuilderFactory
        );
    }

    private class MyUserCursor implements UserCursor {

        private final ToroCursor cursor;

        private MyUserCursor(ToroCursor cursor) {
            this.cursor = cursor;
        }

        @Override
        public CursorId getId() {
            return cursor.getId();
        }

        @Override
        public boolean hasTimeout() {
            return cursor.hasTimeout();
        }

        @Override
        public boolean hasLimit() {
            return cursor.hasLimit();
        }

        @Override
        public int getLimit() throws ToroImplementationException {
            return cursor.getLimit();
        }

        @Override
        public boolean isAutoclosable() {
            return cursor.isAutoclosable();
        }

        @Override
        public FluentIterable<ToroDocument> readAll() throws ClosedToroCursorException {
            return cursor.readAll(executor);
        }

        @Override
        public FluentIterable<ToroDocument> read(int limit) throws ClosedToroCursorException {
            return cursor.read(executor, limit);
        }

        @Override
        public void close() {
            cursor.close(executor);
        }

        @Override
        public boolean isClosed() {
            return cursor.isClosed();
        }

        @Override
        public int getPosition() throws ClosedToroCursorException {
            return cursor.getPosition(executor);
        }

        @Override
        public int getMaxElements() throws UnknownMaxElementsException {
            return cursor.getMaxElements();
        }
        
    }
}
