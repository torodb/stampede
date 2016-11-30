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
import com.torodb.core.concurrent.ConcurrentToolsFactory;
import com.torodb.core.dsl.backend.BackendTransactionJobFactory;
import com.torodb.torod.TorodBundle;
import com.torodb.torod.TorodBundleFactory;
import com.torodb.torod.TorodServer;
import com.torodb.torod.impl.sql.SqlTorodServer;
import com.torodb.torod.pipeline.InsertPipelineFactory;
import com.torodb.torod.pipeline.impl.AkkaInsertPipelineFactory;
import com.torodb.torod.pipeline.impl.DefaultInsertPipelineFactory;
import com.torodb.torod.pipeline.impl.SameThreadInsertPipeline;

import java.util.concurrent.ThreadFactory;

/**
 *
 */
public class SqlTorodModule extends PrivateModule {

  @Override
  protected void configure() {
    install(new FactoryModuleBuilder()
        .implement(SameThreadInsertPipeline.class, SameThreadInsertPipeline.class)
        .build(SameThreadInsertPipeline.Factory.class)
    );
    bind(InsertPipelineFactory.class)
        .to(DefaultInsertPipelineFactory.class)
        .in(Singleton.class);

    install(new FactoryModuleBuilder()
        .implement(TorodBundle.class, TorodBundle.class)
        .build(TorodBundleFactory.class)
    );
    expose(TorodBundleFactory.class);

    bind(TorodServer.class)
        .to(SqlTorodServer.class)
        .in(Singleton.class);
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
