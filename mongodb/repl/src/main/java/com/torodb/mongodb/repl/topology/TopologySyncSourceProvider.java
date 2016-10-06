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

package com.torodb.mongodb.repl.topology;

import com.eightkdata.mongowp.OpTime;
import com.google.common.net.HostAndPort;
import com.torodb.mongodb.repl.SyncSourceProvider;
import com.torodb.mongodb.repl.exceptions.NoSyncSourceFoundException;
import java.util.Optional;
import java.util.function.Supplier;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 *
 */
@Singleton
public class TopologySyncSourceProvider implements SyncSourceProvider {

    private final TopologyService topologyService;

    @Inject
    public TopologySyncSourceProvider(TopologyService topologyService) {
        this.topologyService = topologyService;
    }

    @Override
    public HostAndPort newSyncSource() throws NoSyncSourceFoundException {
        return newSyncSource(Optional.empty(), NoSyncSourceFoundException::new);
    }

    @Override
    public HostAndPort newSyncSource(OpTime lastFetchedOpTime) throws NoSyncSourceFoundException {
        return newSyncSource(Optional.of(lastFetchedOpTime), () -> new NoSyncSourceFoundException(lastFetchedOpTime));
    }

    private HostAndPort newSyncSource(Optional<OpTime> minOpTime, Supplier<NoSyncSourceFoundException> exSupplier) throws NoSyncSourceFoundException {
        return topologyService.chooseNewSyncSource(minOpTime)
                .join()
                .orElseThrow(exSupplier);
    }

    @Override
    public Optional<HostAndPort> getLastUsedSyncSource() {
        return topologyService.getLastUsedSyncSource().join();
    }

}
