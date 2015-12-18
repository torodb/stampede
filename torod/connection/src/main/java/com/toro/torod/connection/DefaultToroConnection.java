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

import com.torodb.torod.core.Session;
import com.torodb.torod.core.config.DocumentBuilderFactory;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.core.connection.ToroTransaction;
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
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;
import javax.json.JsonObject;

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
    public UserCursor<ToroDocument> openUnlimitedCursor(
            String collection, 
            QueryCriteria queryCriteria, 
            Projection projection, 
            int numberToSkip, 
            boolean autoclose, 
            boolean hasTimeout) throws NotAutoclosableCursorException {
        return new MyUserCursor<ToroDocument>(
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
    public UserCursor<ToroDocument> openLimitedCursor(
            String collection, 
            QueryCriteria queryCriteria, 
            Projection projection, 
            int numberToSkip, 
            int limit, 
            boolean autoclose, 
            boolean hasTimeout) throws ToroException {
        return new MyUserCursor<ToroDocument>(
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
    public UserCursor<CollectionMetainfo> openCollectionsMetainfoCursor() {
        return new MyUserCursor<CollectionMetainfo>(
                cursorManager.openCollectionsMetainfoCursor(executor)
        );
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
    public ToroTransaction createTransaction() throws ImplementationDbException {
        SessionTransaction sessionTransaction = executor.createTransaction();
        return new DefaultToroTransaction(
                cache,
                this, 
                sessionTransaction, 
                d2r, 
                executor, 
                documentBuilderFactory
        );
    }

    private class MyUserCursor<E> implements UserCursor<E> {

        private final ToroCursor<E> cursor;

        public MyUserCursor(ToroCursor<E> cursor) {
            this.cursor = cursor;
        }
        
        @Override
        public Class<? extends E> getType() {
            return cursor.getType();
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
        public List<E> readAll() throws ClosedToroCursorException {
            return cursor.readAll(executor);
        }

        @Override
        public List<E> read(int limit) throws ClosedToroCursorException {
            return cursor.read(executor, limit);
        }

        @Override
        public void close() {
            cursor.close(executor);
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
