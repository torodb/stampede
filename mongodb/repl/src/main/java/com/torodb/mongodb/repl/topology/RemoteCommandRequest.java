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
 * along with mongodb-layer. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.mongodb.repl.topology;

import com.google.common.net.HostAndPort;
import java.time.Duration;
import javax.annotation.Nullable;

/**
 *
 */
public class RemoteCommandRequest<Arg> {

    private final HostAndPort target;
    private final String dbname;
    private final Arg cmdObj;
    private final @Nullable Duration timeout;

    public RemoteCommandRequest(HostAndPort target, String dbname, Arg cmdObj) {
        this.target = target;
        this.dbname = dbname;
        this.cmdObj = cmdObj;
        this.timeout = null;
    }

    public RemoteCommandRequest(HostAndPort target, String dbname, Arg cmdObj, Duration timeout) {
        this.target = target;
        this.dbname = dbname;
        this.cmdObj = cmdObj;
        this.timeout = timeout;
    }

    public HostAndPort getTarget() {
        return target;
    }

    public String getDbname() {
        return dbname;
    }

    public Arg getCmdObj() {
        return cmdObj;
    }

    /**
     * @return the max timeout or null if there is no limit
     */
    @Nullable
    public Duration getTimeout() {
        return timeout;
    }

}
