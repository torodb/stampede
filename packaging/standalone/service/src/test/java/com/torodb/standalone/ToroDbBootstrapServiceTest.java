package com.torodb.standalone;


import java.time.Clock;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.util.concurrent.Service;
import com.torodb.packaging.config.model.generic.LogLevel;
import com.torodb.packaging.config.util.ConfigUtils;
import com.torodb.standalone.config.model.Config;
import com.torodb.standalone.config.model.backend.derby.Derby;

/**
 *
 * @author gortiz
 */
public class ToroDbBootstrapServiceTest {

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
    public void testCreateStampedeService() {
        ToroDbBootstrap.createStandaloneService(config, Clock.systemUTC());
    }

    @Test
    @Ignore(value = "The test is not working properly")
    public void testCreateStampedeService_run() {
        Service stampedeService = ToroDbBootstrap.createStandaloneService(
                config,
                Clock.systemUTC());
        stampedeService.startAsync();
        stampedeService.awaitRunning();

        stampedeService.stopAsync();
        stampedeService.awaitTerminated();
    }

}
