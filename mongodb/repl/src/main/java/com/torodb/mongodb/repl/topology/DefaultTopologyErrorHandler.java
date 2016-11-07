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

import com.torodb.core.supervision.Supervisor;
import javax.inject.Inject;
import com.torodb.mongodb.repl.guice.MongoDbRepl;

/**
 *
 */
public class DefaultTopologyErrorHandler implements TopologyErrorHandler {
    private final Supervisor replSupervisor;

    @Inject
    public DefaultTopologyErrorHandler(@MongoDbRepl Supervisor replSupervisor) {
        this.replSupervisor = replSupervisor;
    }

    @Override
    public boolean sendHeartbeatError(Throwable t) {
        return true;
    }

    @Override
    public boolean reciveHeartbeatError(Throwable t) {
        return true;
    }
}
