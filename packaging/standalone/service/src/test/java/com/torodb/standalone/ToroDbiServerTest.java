package com.torodb.standalone;


import java.time.Clock;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import com.torodb.packaging.config.model.generic.LogLevel;
import com.torodb.packaging.config.model.protocol.mongo.Replication;
import com.torodb.packaging.config.model.protocol.mongo.Role;
import com.torodb.packaging.config.util.ConfigUtils;
import com.torodb.standalone.config.model.Config;
import com.torodb.standalone.config.model.backend.derby.Derby;

/**
 *
 * @author gortiz
 */
public class ToroDbiServerTest {

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
        config.getBackend().as(Derby.class).setPassword("torodb");
        config.getGeneric().setLogLevel(LogLevel.TRACE);

        ConfigUtils.validateBean(config);
    }

    @Test
    public void testCreate() {
        ToroDbiServer.create(config, Clock.systemUTC());
    }
}
