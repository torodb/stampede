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
 * along with torod. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.torod;

import com.google.inject.assistedinject.Assisted;
import com.torodb.core.annotations.TorodbIdleService;
import com.torodb.core.modules.AbstractBundle;
import com.torodb.core.supervision.Supervisor;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ThreadFactory;
import javax.inject.Inject;
import com.torodb.core.backend.BackendBundle;
import com.torodb.core.backend.SnapshotUpdater;
import com.torodb.core.d2r.ReservedIdGenerator;
import com.torodb.core.modules.Bundle;
import com.torodb.core.transaction.metainf.MetainfoRepository;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
public class TorodBundle extends AbstractBundle {

    private static final Logger LOGGER = LogManager.getLogger(TorodBundle.class);
    private final TorodServer torodServer;
    private final BackendBundle backendBundle;
    private final MetainfoRepository metainfoRepository;
    private final SnapshotUpdater snapshotUpdater;
    private final ReservedIdGenerator reservedIdGenerator;

    @Inject
    public TorodBundle(@TorodbIdleService ThreadFactory threadFactory,
            @Assisted Supervisor supervisor, TorodServer torodServer,
            @Assisted BackendBundle backendBundle, SnapshotUpdater snapshotUpdater,
            MetainfoRepository metainfoRepository,
            ReservedIdGenerator reservedIdGenerator) {
        super(threadFactory, supervisor);
        this.torodServer = torodServer;
        this.backendBundle = backendBundle;
        this.snapshotUpdater = snapshotUpdater;
        this.metainfoRepository = metainfoRepository;
        this.reservedIdGenerator = reservedIdGenerator;
    }

    @Override
    protected void postDependenciesStartUp() throws Exception {
        LOGGER.trace("Loading backend metadata...");
        snapshotUpdater.updateSnapshot(metainfoRepository);

        LOGGER.trace("Reading last used rids...");
        reservedIdGenerator.startAsync();
        reservedIdGenerator.awaitRunning();

        torodServer.startAsync();
        torodServer.awaitRunning();
    }

    @Override
    protected void preDependenciesShutDown() throws Exception {
        torodServer.stopAsync();
        torodServer.awaitTerminated();

        reservedIdGenerator.stopAsync();
        reservedIdGenerator.awaitTerminated();
    }

    @Override
    public Collection<Bundle> getDependencies() {
        return Collections.singleton(backendBundle);
    }

    public TorodServer getTorodServer() {
        return torodServer;
    }

}