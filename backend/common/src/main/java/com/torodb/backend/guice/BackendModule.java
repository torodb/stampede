
package com.torodb.backend.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.torodb.backend.BackendImpl;
import com.torodb.backend.jobs.BackendConnectionJobFactoryImpl;
import com.torodb.core.backend.Backend;
import com.torodb.core.dsl.backend.BackendConnectionJobFactory;

/**
 *
 */
public class BackendModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(BackendConnectionJobFactory.class)
                .to(BackendConnectionJobFactoryImpl.class)
                .in(Singleton.class);

        bind(Backend.class)
                .to(BackendImpl.class)
                .asEagerSingleton();
    }

}

