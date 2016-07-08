package com.torodb.packaging;

import com.torodb.packaging.config.model.Config;
import com.torodb.packaging.config.model.backend.derby.Derby;
import com.torodb.packaging.config.model.generic.LogLevel;
import com.torodb.packaging.config.util.ConfigUtils;
import java.time.Clock;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author gortiz
 */
public class ToroDBServerTest {

    private Config config;

    @Before
    public void setUp() {
        config = new Config();

        config.getProtocol().getMongo().setReplication(null);
        config.getBackend().setBackendImplementation(new Derby());
        config.getBackend().asDerby().setPassword("torodb");
        config.getGeneric().setLogLevel(LogLevel.TRACE);

        ConfigUtils.validateBean(config);
    }

    @Test
    public void testCreate() {
        ToroDBServer.create(config, Clock.systemUTC());
    }

    @Test
    public void testInitiate() throws TimeoutException {
        ToroDBServer server = ToroDBServer.create(config, Clock.systemUTC());

        server.startAsync();
        server.awaitRunning(10, TimeUnit.SECONDS);

        server.stopAsync();
        server.awaitTerminated(10, TimeUnit.SECONDS);
    }
}
