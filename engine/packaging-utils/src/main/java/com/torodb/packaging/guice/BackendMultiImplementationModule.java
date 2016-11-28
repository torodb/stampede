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

package com.torodb.packaging.guice;

import com.google.inject.PrivateModule;
import com.torodb.backend.DbBackendService;
import com.torodb.backend.SqlHelper;
import com.torodb.backend.SqlInterface;
import com.torodb.backend.meta.SchemaUpdater;
import com.torodb.core.backend.BackendBundleFactory;
import com.torodb.core.backend.IdentifierConstraints;
import com.torodb.core.backend.SnapshotUpdater;
import com.torodb.core.d2r.ReservedIdGenerator;
import com.torodb.core.dsl.backend.BackendTransactionJobFactory;
import com.torodb.packaging.config.model.backend.BackendImplementation;
import com.torodb.packaging.config.model.backend.ConnectionPoolConfig;
import com.torodb.packaging.config.model.backend.CursorConfig;

public class BackendMultiImplementationModule extends PrivateModule {

  private final CursorConfig cursorConfig;
  private final ConnectionPoolConfig connectionPoolConfig;
  private final BackendImplementation backendImplementation;
  private final BackendImplementationModule<?, ?>[] backendModules;

  public BackendMultiImplementationModule(
      CursorConfig cursorConfig,
      ConnectionPoolConfig connectionPoolConfig,
      BackendImplementation backendImplementation,
      BackendImplementationModule<?, ?>... backendImplementationModules) {
    this.cursorConfig = cursorConfig;
    this.connectionPoolConfig = connectionPoolConfig;
    this.backendImplementation = backendImplementation;
    this.backendModules = backendImplementationModules;
  }

  @Override
  protected void configure() {
    bind(CursorConfig.class).toInstance(cursorConfig);
    bind(ConnectionPoolConfig.class).toInstance(connectionPoolConfig);
    for (BackendImplementationModule<?, ?> backendImplementationModule : backendModules) {
      if (backendImplementationModule.isForConfiguration(backendImplementation)) {
        backendImplementationModule.setConfiguration(backendImplementation);
        install(backendImplementationModule);
        expose(SqlHelper.class);
        expose(SqlInterface.class);
        expose(SchemaUpdater.class);
        expose(DbBackendService.class);
        expose(IdentifierConstraints.class);
        expose(BackendBundleFactory.class);
        expose(BackendTransactionJobFactory.class);
        expose(ReservedIdGenerator.class);
        expose(SnapshotUpdater.class);
        break;
      }
    }
  }
}
