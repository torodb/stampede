/*
 * ToroDB
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.backend.guice;

import com.google.inject.PrivateModule;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.torodb.backend.BackendBundleImpl;
import com.torodb.backend.BackendServiceImpl;
import com.torodb.backend.DslContextFactory;
import com.torodb.backend.DslContextFactoryImpl;
import com.torodb.backend.KvMetainfoHandler;
import com.torodb.backend.SqlHelper;
import com.torodb.backend.SqlInterface;
import com.torodb.backend.SqlInterfaceDelegate;
import com.torodb.backend.jobs.BackendConnectionJobFactoryImpl;
import com.torodb.backend.meta.SnapshotUpdaterImpl;
import com.torodb.backend.rid.ReservedIdGeneratorImpl;
import com.torodb.backend.rid.ReservedIdInfoFactory;
import com.torodb.backend.rid.ReservedIdInfoFactoryImpl;
import com.torodb.core.backend.BackendBundle;
import com.torodb.core.backend.BackendBundleFactory;
import com.torodb.core.backend.SnapshotUpdater;
import com.torodb.core.d2r.ReservedIdGenerator;
import com.torodb.core.dsl.backend.BackendTransactionJobFactory;

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
