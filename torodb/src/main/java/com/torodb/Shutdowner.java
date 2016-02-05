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

package com.torodb;

import com.eightkdata.mongowp.server.wp.NettyMongoServer;
import com.google.inject.Provider;
import com.torodb.torod.core.Torod;
import com.torodb.torod.core.backend.DbBackend;
import com.torodb.torod.mongodb.repl.ReplCoordinator;
import com.torodb.torod.mongodb.repl.ReplCoordinator.ReplCoordinatorOwnerCallback;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class Shutdowner implements ReplCoordinatorOwnerCallback {
    private static final Logger LOGGER
            = LoggerFactory.getLogger(Shutdowner.class);
    private final Provider<DbBackend> dbBackend;
    private final Provider<Torod> torod;
    private final Provider<NettyMongoServer> server;
    private final Provider<ReplCoordinator> replCoord;
    private final Provider<ExecutorService> executorService;

    @Inject
    public Shutdowner(Provider<DbBackend> dbBackend, Provider<Torod> torod, Provider<NettyMongoServer> server, Provider<ReplCoordinator> replCoord, Provider<ExecutorService> executorService) {
        this.dbBackend = dbBackend;
        this.torod = torod;
        this.server = server;
        this.replCoord = replCoord;
        this.executorService = executorService;
    }

    public void shutdown() {
        server.get().stopAsync().awaitTerminated();
        torod.get().shutdown();
        dbBackend.get().shutdown();
        ReplCoordinator replCoordinatorInstance = replCoord.get();
        replCoordinatorInstance.stopAsync();
        replCoordinatorInstance.awaitTerminated();

        shutdownExecutor();
    }

    @Override
    public void replCoordStopped() {
        server.get().stopAsync().awaitTerminated();
        torod.get().shutdown();
        dbBackend.get().shutdown();
        
        shutdownExecutor();
    }

    private void shutdownExecutor() {
        ExecutorService executorServiceInstance = executorService.get();
        executorServiceInstance.shutdown();
        boolean executorTerminated = false;
        try {
            executorTerminated = executorServiceInstance.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
        }

        if (!executorTerminated) {
            List<Runnable> notExecutedTasks = executorServiceInstance.shutdownNow();
            LOGGER.error("The executor service didn't stop in the expected time. {} task were waiting", notExecutedTasks);
        }
    }

}
