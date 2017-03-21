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

package com.torodb.torod.guice;

import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.torodb.core.backend.BackendExtInt;
import com.torodb.core.backend.BackendService;
import com.torodb.core.concurrent.ConcurrentToolsFactory;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.d2r.ReservedIdGenerator;
import com.torodb.core.dsl.backend.BackendTransactionJobFactory;
import com.torodb.core.guice.EssentialToDefaultModule;
import com.torodb.core.transaction.metainf.impl.guice.D2RModule;
import com.torodb.torod.SqlTorodConfig;
import com.torodb.torod.TorodServer;
import com.torodb.torod.impl.sql.SqlTorodServer;
import com.torodb.torod.pipeline.InsertPipelineFactory;
import com.torodb.torod.pipeline.impl.AkkaInsertPipelineFactory;
import com.torodb.torod.pipeline.impl.DefaultInsertPipelineFactory;
import com.torodb.torod.pipeline.impl.SameThreadInsertPipeline;

import java.util.concurrent.ThreadFactory;

public class SqlTorodModule extends PrivateModule {

  private final SqlTorodConfig config;

  public SqlTorodModule(SqlTorodConfig config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    expose(TorodServer.class);
    expose(InsertPipelineFactory.class);

    bindConfig();

    install(new EssentialToDefaultModule());

    install(new D2RModule());

    install(new FactoryModuleBuilder()
        .implement(SameThreadInsertPipeline.class, SameThreadInsertPipeline.class)
        .build(SameThreadInsertPipeline.Factory.class)
    );
    bind(InsertPipelineFactory.class)
        .to(DefaultInsertPipelineFactory.class)
        .in(Singleton.class);

    bind(TorodServer.class)
        .to(SqlTorodServer.class)
        .in(Singleton.class);
  }

  private void bindConfig() {
    BackendExtInt backendExtInt = config.getBackendBundle().getExternalInterface();

    bind(BackendService.class)
        .toInstance(backendExtInt.getBackendService());
    bind(IdentifierFactory.class)
        .toInstance(backendExtInt.getIdentifierFactory());
    bind(BackendTransactionJobFactory.class)
        .toInstance(backendExtInt.getBackendTransactionJobFactory());
    bind(ReservedIdGenerator.class)
        .toInstance(backendExtInt.getReservedIdGenerator());
  }

  @Provides
  @Singleton
  AkkaInsertPipelineFactory createConcurrentPipelineFactory(
      ThreadFactory threadFactory,
      ConcurrentToolsFactory concurrentToolsFactory,
      BackendTransactionJobFactory backendTransactionJobFactory) {

    return new AkkaInsertPipelineFactory(threadFactory,
        concurrentToolsFactory, backendTransactionJobFactory, 100);
  }

}
