
package com.torodb.standalone;

import javax.inject.Singleton;

import com.google.inject.Exposed;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.torodb.core.backend.BackendBundle;
import com.torodb.core.backend.BackendService;
import com.torodb.core.supervision.Supervisor;
import com.torodb.d2r.guice.D2RModule;
import com.torodb.mongodb.commands.TorodbCommandsLibrary;
import com.torodb.mongodb.core.MongodServer;
import com.torodb.mongodb.guice.MongoLayerModule;
import com.torodb.mongodb.language.ObjectIdFactory;
import com.torodb.torod.TorodBundle;
import com.torodb.torod.TorodBundleFactory;
import com.torodb.torod.guice.SqlTorodModule;

/**
 *
 */
public class ToroDbRuntimeModule extends PrivateModule {

    private final BackendBundle backend;
    private final Supervisor supervisor;

    public ToroDbRuntimeModule(BackendBundle backend,
            Supervisor supervisor) {
        this.backend = backend;
        this.supervisor = supervisor;
    }

    @Override
    protected void configure() {
        binder().requireExplicitBindings();
        bind(BackendService.class)
                .toInstance(backend.getBackendService());
        expose(BackendService.class);
        
        install(new D2RModule());
        install(new SqlTorodModule());
        install(new MongoLayerModule());
        expose(MongodServer.class);
        expose(TorodbCommandsLibrary.class);
        expose(ObjectIdFactory.class);
    }

    @Provides @Singleton @Exposed
    TorodBundle createTorodBundle(TorodBundleFactory factory) {
        return factory.createBundle(supervisor, backend);
    }

}
