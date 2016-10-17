package com.torodb.mongodb.guice;

import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.torodb.mongodb.core.MongodServer;
import com.torodb.torod.TorodBundle;
import com.torodb.torod.TorodServer;

/**
 *
 */
public class MongoLayerModule extends PrivateModule {

    @Override
    protected void configure() {
        bind(MongodServer.class)
                .in(Singleton.class);
        expose(MongodServer.class);
    }

    @Provides
    TorodServer getTorodServer(TorodBundle torodBundle) {
        return torodBundle.getTorodServer();
    }
}
