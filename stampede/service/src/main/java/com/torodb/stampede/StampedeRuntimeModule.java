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

package com.torodb.stampede;

import com.google.inject.Exposed;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.torodb.core.backend.BackendBundle;
import com.torodb.core.backend.BackendService;
import com.torodb.d2r.guice.D2RModule;
import com.torodb.mongodb.repl.ConsistencyHandler;
import com.torodb.torod.TorodBundle;
import com.torodb.torod.TorodBundleFactory;
import com.torodb.torod.guice.SqlTorodModule;

import javax.inject.Singleton;

/**
 *
 */
public class StampedeRuntimeModule extends PrivateModule {

  private final BackendBundle backend;
  private final StampedeService stampedeService;
  private final ConsistencyHandler consistencyHandler;

  public StampedeRuntimeModule(BackendBundle backend,
      StampedeService stampedeService,
      ConsistencyHandler consistencyHandler) {
    this.backend = backend;
    this.stampedeService = stampedeService;
    this.consistencyHandler = consistencyHandler;
  }

  @Override
  protected void configure() {
    binder().requireExplicitBindings();
    bind(ConsistencyHandler.class)
        .toInstance(consistencyHandler);
    expose(ConsistencyHandler.class);
    bind(BackendService.class)
        .toInstance(backend.getBackendService());
    expose(BackendService.class);

    install(new D2RModule());
    install(new SqlTorodModule());
  }

  @Provides
  @Singleton
  @Exposed
  TorodBundle createTorodBundle(TorodBundleFactory factory) {
    return factory.createBundle(stampedeService, backend);
  }

}
