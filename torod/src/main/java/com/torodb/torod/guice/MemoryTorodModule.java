
package com.torodb.torod.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.torodb.torod.TorodServer;
import com.torodb.torod.impl.memory.MemoryTorodServer;

/**
 *
 */
public class MemoryTorodModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(MemoryTorodServer.class)
                .in(Singleton.class);
        
        bind(TorodServer.class)
                .to(MemoryTorodServer.class);
    }

}
