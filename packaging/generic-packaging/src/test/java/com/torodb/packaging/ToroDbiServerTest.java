package com.torodb.packaging;

import com.torodb.packaging.config.model.Config;
import com.torodb.packaging.config.model.backend.derby.Derby;
import com.torodb.packaging.config.model.generic.LogLevel;
import com.torodb.packaging.config.model.protocol.mongo.Replication;
import com.torodb.packaging.config.model.protocol.mongo.Role;
import com.torodb.packaging.config.util.ConfigUtils;
import java.time.Clock;
import java.util.Collections;
import org.junit.Before;
import org.junit.Test;

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
        config.getBackend().asDerby().setPassword("torodb");
        config.getGeneric().setLogLevel(LogLevel.TRACE);

        ConfigUtils.validateBean(config);
    }

    @Test
    public void testCreate() {
        ToroDbiServer.create(config, Clock.systemUTC());
    }
}
