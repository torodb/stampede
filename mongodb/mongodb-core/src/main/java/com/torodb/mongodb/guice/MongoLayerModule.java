package com.torodb.mongodb.guice;

import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.torodb.mongodb.commands.CommandImplementionsModule;
import com.torodb.mongodb.commands.TorodbCommandsLibrary;
import com.torodb.mongodb.core.MongodMetrics;
import com.torodb.mongodb.core.MongodServer;
import com.torodb.mongodb.language.ObjectIdFactory;
import com.torodb.torod.TorodBundle;
import com.torodb.torod.TorodServer;

/**
 *
 */
public class MongoLayerModule extends PrivateModule {
    
    private final Module commandImplementationModule;
    
    public MongoLayerModule() {
        this(new CommandImplementionsModule());
    }
    
    public MongoLayerModule(Module commandImplementationModule) {
        this.commandImplementationModule = commandImplementationModule;
    }

    @Override
    protected void configure() {
        bind(MongodServer.class)
                .in(Singleton.class);
        expose(MongodServer.class);

        install(commandImplementationModule);

        bind(ObjectIdFactory.class)
                .in(Singleton.class);
        expose(ObjectIdFactory.class);

        bind(TorodbCommandsLibrary.class)
                .in(Singleton.class);
        expose(TorodbCommandsLibrary.class);

        bind(MongodMetrics.class)
                .in(Singleton.class);
    }

    @Provides
    TorodServer getTorodServer(TorodBundle torodBundle) {
        return torodBundle.getTorodServer();
    }
}
