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

import com.torodb.torod.core.Torod;
import com.torodb.torod.core.backend.DbBackend;
import com.torodb.torod.core.config.DocumentBuilderFactory;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.core.d2r.D2RTranslator;
import com.torodb.torod.core.dbMetaInf.DbMetaInformationCache;
import com.torodb.torod.core.dbWrapper.DbWrapper;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.exceptions.TorodStartupException;
import com.torodb.torod.core.executor.ExecutorFactory;
import javax.inject.Inject;

/**
 *
 */
public class DefaultTorod implements Torod {

    private final D2RTranslator d2r;
    private final DbMetaInformationCache cache;
    private final ExecutorFactory executorFactory;
    private final DbWrapper dbWrapper;
    private final DocumentBuilderFactory documentBuilderFactory;
    private final DefaultCursorManager cursorManager;

    @Inject
    public DefaultTorod(
            D2RTranslator d2RTranslator,
            DbMetaInformationCache cache,
            ExecutorFactory executorFactory,
            DbWrapper dbWrapper,
            DocumentBuilderFactory documentBuilderFactory,
            DbBackend dbBackend) {
        this.d2r = d2RTranslator;
        this.cache = cache;
        this.executorFactory = executorFactory;
        this.dbWrapper = dbWrapper;
        this.documentBuilderFactory = documentBuilderFactory;
        this.cursorManager = new DefaultCursorManager(dbBackend, d2r);
    }

    @Override
    public void start() {
        try {
			executorFactory.initialize();
		} catch (ImplementationDbException e) {
			throw new TorodStartupException(e.getMessage());
		}
        cache.initialize();
        d2r.initialize();
    }

    @Override
    public ToroConnection openConnection() {
        return new DefaultToroConnection(
                cursorManager,
                d2r, 
                executorFactory, 
                dbWrapper,
                documentBuilderFactory,
                cache
        );
    }

    @Override
    public void shutdown() {
        //TODO: TEMPORAL IMPLEMENTATION
        d2r.shutdown();
        cache.shutdown();
        executorFactory.shutdown();
    }

    @Override
    public void shutdownNow() {
        d2r.shutdownNow();
        cache.shutdownNow();
        executorFactory.shutdownNow();
    }
}
