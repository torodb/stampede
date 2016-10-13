package com.torodb.stampede;


import java.time.Clock;

import org.junit.Before;
import org.junit.Test;

import com.torodb.packaging.config.model.backend.derby.Derby;
import com.torodb.packaging.config.model.generic.LogLevel;
import com.torodb.packaging.config.model.protocol.mongo.Role;
import com.torodb.packaging.config.util.ConfigUtils;
import com.torodb.stampede.config.model.Config;
import com.torodb.stampede.config.model.replication.Replication;

/**
 *
 * @author gortiz
 */
public class StampedeServerTest {

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
        config.getBackend().asDerby().setPassword("torodb");
        config.getGeneric().setLogLevel(LogLevel.TRACE);

        ConfigUtils.validateBean(config);
    }

    @Test
    public void testCreate() {
        StampedeServer.create(config, Clock.systemUTC());
    }
}
