package com.torodb.packaging.stampede;

import com.google.common.util.concurrent.Service;
import com.torodb.packaging.config.model.Config;
import com.torodb.packaging.config.model.backend.derby.Derby;
import com.torodb.packaging.config.model.generic.LogLevel;
import com.torodb.packaging.config.model.protocol.mongo.Replication;
import com.torodb.packaging.config.model.protocol.mongo.Role;
import com.torodb.packaging.config.util.ConfigUtils;
import java.time.Clock;
import java.util.Collections;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

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

        config.getProtocol().getMongo().setReplication(Collections.singletonList(
                replication
        ));
        config.getBackend().setBackendImplementation(new Derby());
        config.getBackend().asDerby().setPassword("torodb");
        config.getGeneric().setLogLevel(LogLevel.TRACE);

        ConfigUtils.validateBean(config);
    }

    @Test
    public void testCreateStampedeService() {
        StampedeBootstrap.createStampedeService(config, Clock.systemUTC());
    }

    @Test
    @Ignore
    public void testCreateStampedeService_run() {
        Service stampedeService = StampedeBootstrap.createStampedeService(
                config,
                Clock.systemUTC());
        stampedeService.startAsync();
        stampedeService.awaitRunning();
    }

}
