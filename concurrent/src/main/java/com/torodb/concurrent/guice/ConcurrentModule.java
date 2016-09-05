
package com.torodb.concurrent.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.torodb.concurrent.DefaultConcurrentToolsFactory;
import com.torodb.core.concurrent.ConcurrentToolsFactory;

/**
 *
 */
public class ConcurrentModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(ConcurrentToolsFactory.class)
                .to(DefaultConcurrentToolsFactory.class)
                .in(Singleton.class);
    }

}
