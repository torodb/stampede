
package com.torodb.mongodb.core;

import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;

/**
 *
 */
public class MongodServerConfig {

    private final HostAndPort hostAndPort;

    public MongodServerConfig(HostAndPort hostAndPort) {
        this.hostAndPort = hostAndPort;
        Preconditions.checkArgument(hostAndPort.hasPort(), "The host and port of a given mongod server must have a port");
    }

    public HostAndPort getHostAndPort() {
        return hostAndPort;
    }

}
