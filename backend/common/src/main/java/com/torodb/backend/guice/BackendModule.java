
package com.torodb.backend.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.torodb.backend.BackendImpl;
import com.torodb.backend.jobs.BackendConnectionJobFactoryImpl;
import com.torodb.backend.rid.MaxRowIdFactory;
import com.torodb.backend.rid.ReservedIdContainer;
import com.torodb.backend.rid.ReservedIdInfoFactory;
import com.torodb.core.backend.Backend;
import com.torodb.core.d2r.RidGenerator;
import com.torodb.core.dsl.backend.BackendTransactionJobFactory;

/**
 *
 */
public class BackendModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(BackendTransactionJobFactory.class)
                .to(BackendConnectionJobFactoryImpl.class)
                .in(Singleton.class);

        bind(Backend.class)
                .to(BackendImpl.class)
                .asEagerSingleton();
        
        bind(ReservedIdInfoFactory.class)
                .to(MaxRowIdFactory.class)
                .asEagerSingleton();
        
        bind(RidGenerator.class)
                .to(ReservedIdContainer.class)
                .asEagerSingleton();
    }

}

