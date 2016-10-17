package com.torodb.standalone;


import java.time.Clock;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Before;
import org.junit.Test;

import com.torodb.packaging.config.model.generic.LogLevel;
import com.torodb.packaging.config.util.ConfigUtils;
import com.torodb.standalone.config.model.Config;
import com.torodb.standalone.config.model.backend.derby.Derby;

/**
 *
 * @author gortiz
 */
public class ToroDbServerTest {

    private Config config;

    @Before
    public void setUp() {
        config = new Config();

        config.getProtocol().getMongo().setReplication(null);
        config.getBackend().setBackendImplementation(new Derby());
        config.getBackend().as(Derby.class).setPassword("torodb");
        config.getGeneric().setLogLevel(LogLevel.TRACE);

        ConfigUtils.validateBean(config);
    }

    @Test
    public void testCreate() {
        ToroDbServer.create(config, Clock.systemUTC());
    }

    @Test
    public void testInitiate() throws TimeoutException {
        ToroDbServer server = ToroDbServer.create(config, Clock.systemUTC());

        server.startAsync();
        server.awaitRunning(10, TimeUnit.SECONDS);

        server.stopAsync();
        server.awaitTerminated(10, TimeUnit.SECONDS);
    }
}
