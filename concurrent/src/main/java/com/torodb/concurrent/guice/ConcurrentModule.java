
package com.torodb.concurrent.guice;

import com.google.inject.PrivateModule;
import com.google.inject.Singleton;
import com.torodb.concurrent.DefaultConcurrentToolsFactory;
import com.torodb.core.concurrent.ConcurrentToolsFactory;

/**
 *
 */
public class ConcurrentModule extends PrivateModule {

    @Override
    protected void configure() {
        bind(ConcurrentToolsFactory.class)
                .to(DefaultConcurrentToolsFactory.class)
                .in(Singleton.class);
        expose(ConcurrentToolsFactory.class);
    }

}
