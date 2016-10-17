
package com.torodb.torod.guice;

import com.google.inject.PrivateModule;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.torodb.torod.TorodBundle;
import com.torodb.torod.TorodBundleFactory;
import com.torodb.torod.TorodServer;
import com.torodb.torod.impl.memory.MemoryTorodServer;

/**
 *
 */
public class MemoryTorodModule extends PrivateModule {

    @Override
    protected void configure() {
        bind(MemoryTorodServer.class)
                .in(Singleton.class);
        
        bind(TorodServer.class)
                .to(MemoryTorodServer.class);

        install(new FactoryModuleBuilder()
                .implement(TorodBundle.class, TorodBundle.class)
                .build(TorodBundleFactory.class)
        );
        expose(TorodBundleFactory.class);
    }

}
