package com.torodb.stampede;

import java.time.Clock;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.util.concurrent.Service;
import com.torodb.packaging.config.model.generic.LogLevel;
import com.torodb.packaging.config.model.protocol.mongo.Role;
import com.torodb.packaging.config.util.ConfigUtils;
import com.torodb.stampede.config.model.Config;
import com.torodb.stampede.config.model.replication.Replication;

/**
 *
 * @author gortiz
 */
public class StampedeBootstrapTest {

    private Config config;

    @Before
    public void setUp() {
        config = new Config();

        Replication replication = new Replication();
        replication.setRole(Role.HIDDEN_SLAVE);
        replication.setReplSetName("replSetName");
        replication.setSyncSource("localhost:27020");

        config.setReplication(
                replication
        );
        config.getBackend().setBackendImplementation(new Derby());
        config.getBackend().as(Derby.class).setPassword("torodb");
        config.getLogging().setLevel(LogLevel.TRACE);

        ConfigUtils.validateBean(config);
    }

    @Test
    public void testCreateStampedeService() {
        StampedeBootstrap.createStampedeService(config, Clock.systemUTC());
    }

    @Test
    @Ignore(value = "The test is not working properly")
    public void testCreateStampedeService_run() {
        Service stampedeService = StampedeBootstrap.createStampedeService(
                config,
                Clock.systemUTC());
        stampedeService.startAsync();
        stampedeService.awaitRunning();

        stampedeService.stopAsync();
        stampedeService.awaitTerminated();
    }
    
    private class Derby extends com.torodb.packaging.config.model.backend.derby.Derby {
        public Derby() {
            super(
                    "localhost", 
                    1527, 
                    "torod", 
                    "torodb", 
                    null, 
                    System.getProperty("user.home", "/") + "/.toropass", 
                    "toro", 
                    false, 
                    true, 
                    true);
        }
    }

}
