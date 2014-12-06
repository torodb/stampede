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
import com.torodb.torod.core.connection.CursorManager;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.core.connection.ToroTransaction;
import com.torodb.torod.core.cursors.InnerCursorManager;
import com.torodb.torod.core.d2r.D2RTranslator;
import com.torodb.torod.core.dbMetaInf.DbMetaInformationCache;
import com.torodb.torod.core.dbWrapper.DbWrapper;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.executor.ExecutorFactory;
import com.torodb.torod.core.executor.SessionExecutor;
import com.torodb.torod.core.executor.SessionTransaction;
import com.torodb.torod.core.pojos.Database;
import java.util.List;
import java.util.concurrent.Future;
import javax.annotation.concurrent.NotThreadSafe;

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
    private final CursorManager cursorManager;

    DefaultToroConnection(
            D2RTranslator d2RTranslator,
            ExecutorFactory executorFactory,
            DbWrapper dbWrapper,
            InnerCursorManager globalInnerCursorManager,
            DocumentBuilderFactory documentBuilderFactory,
            DbMetaInformationCache cache) {
        this.session = new DefaultSession();
        this.d2r = d2RTranslator;

        this.executor = executorFactory.createSessionExecutor(session);
        this.documentBuilderFactory = documentBuilderFactory;
        this.cache = cache;
        this.cursorManager = new DefaultCursorManager(
                globalInnerCursorManager, 
                dbWrapper, 
                executor, 
                d2r
        );
    }

    @Override
    public Session getSession() {
        return session;
    }

    @Override
    public boolean createCollection(String collection) {
        if(cache.collectionExists(collection)) {
            return false;
        }

        return cache.createCollection(executor, collection);
    }

    @Override
    public void close() {
        executor.close();
    }

    @Override
    public ToroTransaction createTransaction() throws ImplementationDbException {
        SessionTransaction sessionTransaction = executor.createTransaction();
        return new DefaultToroTransaction(
                session, 
                sessionTransaction, 
                d2r, 
                executor, 
                documentBuilderFactory,
                cursorManager
        );
    }

    @Override
    public CursorManager getCursorManager() {
        return cursorManager;
    }

    @Override
    public Future<List<? extends Database>> getDatabases() {
        return executor.getDatabases();
    }

}
