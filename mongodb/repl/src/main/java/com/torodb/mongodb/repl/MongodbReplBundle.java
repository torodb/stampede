/*
 * This file is part of ToroDB.
 *
 * ToroDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ToroDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with repl. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.mongodb.repl;

import com.eightkdata.mongowp.client.core.CachedMongoClientFactory;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.torodb.core.annotations.TorodbIdleService;
import com.torodb.core.modules.AbstractBundle;
import com.torodb.core.supervision.Supervisor;
import com.torodb.mongodb.repl.topology.TopologyService;
import com.torodb.torod.TorodBundle;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ThreadFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.torodb.core.modules.Bundle;
import com.torodb.mongodb.core.MongodServer;
import com.torodb.mongodb.repl.guice.MongoDbReplModule;
import com.torodb.mongodb.repl.guice.MongodbReplConfig;

/**
 *
 */
public class MongodbReplBundle extends AbstractBundle {

    private static final Logger LOGGER
            = LogManager.getLogger(MongodbReplBundle.class);
    private final TorodBundle torodBundle;
    private final TopologyService topologyService;
    private final ReplCoordinator replCoordinator;
    private final OplogManager oplogManager;
    private final MongodServer mongodServer;
    private final CachedMongoClientFactory cachedMongoClientFactory;

    public MongodbReplBundle(TorodBundle torodBundle, Supervisor supervisor,
            MongodbReplConfig config, Injector injector) {
        super(
                injector.getInstance(
                        Key.get(ThreadFactory.class, TorodbIdleService.class)),
                supervisor);
        Injector replInjector = injector.createChildInjector(
                new MongoDbReplModule(config)
        );

        this.torodBundle = torodBundle;
        this.topologyService = replInjector.getInstance(TopologyService.class);
        this.replCoordinator = replInjector.getInstance(ReplCoordinator.class);
        this.oplogManager = replInjector.getInstance(OplogManager.class);
        this.mongodServer = replInjector.getInstance(MongodServer.class);
        this.cachedMongoClientFactory = replInjector.getInstance(CachedMongoClientFactory.class);
    }

    @Override
    protected void postDependenciesStartUp() throws Exception {
        LOGGER.info("Starting replication service");

        mongodServer.startAsync();
        mongodServer.awaitRunning();

        topologyService.startAsync();
        oplogManager.startAsync();

        topologyService.awaitRunning();
        oplogManager.awaitRunning();

        replCoordinator.start().join();

        LOGGER.info("Replication service started");
    }

    @Override
    protected void preDependenciesShutDown() throws Exception {
        LOGGER.info("Shutting down replication service");

        LOGGER.debug("Shutting down replication layer");
        replCoordinator.stop().join();
        oplogManager.stopAsync();
        topologyService.stopAsync();

        oplogManager.awaitTerminated();
        topologyService.awaitTerminated();

        LOGGER.debug("Replication layer has been shutted down");

        mongodServer.stopAsync();

        LOGGER.debug("Closing remote connections");
        cachedMongoClientFactory.invalidateAll();
        LOGGER.debug("Remote connections have been closed");

        mongodServer.awaitTerminated();

        LOGGER.info("Replication service shutted down");
    }

    @Override
    public Collection<Bundle> getDependencies() {
        return Collections.singleton(torodBundle);
    }

    public MongodServer getMongodServer() {
        return mongodServer;
    }
}
