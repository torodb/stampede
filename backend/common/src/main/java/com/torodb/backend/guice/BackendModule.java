
package com.torodb.backend.guice;

import com.google.inject.PrivateModule;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.torodb.backend.*;
import com.torodb.backend.jobs.BackendConnectionJobFactoryImpl;
import com.torodb.backend.meta.SnapshotUpdaterImpl;
import com.torodb.backend.rid.ReservedIdInfoFactoryImpl;
import com.torodb.backend.rid.ReservedIdGeneratorImpl;
import com.torodb.backend.rid.ReservedIdInfoFactory;
import com.torodb.core.backend.BackendBundle;
import com.torodb.core.backend.BackendBundleFactory;
import com.torodb.core.backend.SnapshotUpdater;
import com.torodb.core.dsl.backend.BackendTransactionJobFactory;
import com.torodb.core.d2r.ReservedIdGenerator;

/**
 *
 */
public class BackendModule extends PrivateModule {

    @Override
    protected void configure() {
        bind(SqlInterfaceDelegate.class)
                .in(Singleton.class);
        bind(SqlInterface.class)
                .to(SqlInterfaceDelegate.class);
        expose(SqlInterface.class);
        
        bind(BackendTransactionJobFactory.class)
                .to(BackendConnectionJobFactoryImpl.class)
                .in(Singleton.class);
        expose(BackendTransactionJobFactory.class);

        bind(ReservedIdInfoFactoryImpl.class)
                .in(Singleton.class);
        bind(ReservedIdInfoFactory.class)
                .to(ReservedIdInfoFactoryImpl.class);

        bind(ReservedIdGeneratorImpl.class)
                .in(Singleton.class);
        bind(ReservedIdGenerator.class)
                .to(ReservedIdGeneratorImpl.class);
        expose(ReservedIdGenerator.class);

        bind(DslContextFactoryImpl.class)
                .in(Singleton.class);
        bind(DslContextFactory.class)
                .to(DslContextFactoryImpl.class);

        bind(SnapshotUpdaterImpl.class);
        bind(SnapshotUpdater.class)
                .to(SnapshotUpdaterImpl.class);
        expose(SnapshotUpdater.class);

        install(new FactoryModuleBuilder()
                .implement(BackendBundle.class, BackendBundleImpl.class)
                .build(BackendBundleFactory.class)
        );
        expose(BackendBundleFactory.class);

        bind(SqlHelper.class)
                .in(Singleton.class);
        expose(SqlHelper.class);

        bind(BackendServiceImpl.class)
                .in(Singleton.class);

        bind(KvMetainfoHandler.class);
    }

}

