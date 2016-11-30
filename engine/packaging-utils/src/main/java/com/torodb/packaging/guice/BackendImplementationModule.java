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

import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.torodb.backend.BackendConfiguration;
import com.torodb.backend.DbBackendService;
import com.torodb.backend.SqlHelper;
import com.torodb.backend.SqlInterface;
import com.torodb.backend.guice.BackendModule;
import com.torodb.backend.meta.SchemaUpdater;
import com.torodb.core.backend.BackendBundleFactory;
import com.torodb.core.backend.IdentifierConstraints;
import com.torodb.core.backend.SnapshotUpdater;
import com.torodb.core.d2r.ReservedIdGenerator;
import com.torodb.core.dsl.backend.BackendTransactionJobFactory;
import com.torodb.packaging.config.model.backend.BackendImplementation;

import java.util.function.Supplier;

@SuppressWarnings("checkstyle:LineLength")
public abstract class BackendImplementationModule<T extends BackendImplementation, C extends BackendConfiguration>
    extends PrivateModule {

  private final Class<T> configurationClass;
  private final Class<C> backendConfigurationClass;
  private final Class<? extends C> backendConfigurationMapperClass;
  private final Supplier<Module> backendModuleSupplier;

  private BackendImplementation backendImplementation;

  public BackendImplementationModule(Class<T> configurationClass,
      Class<C> backendConfigurationClass,
      Class<? extends C> backendConfigurationMapperClass,
      Supplier<Module> backendModuleSupplier) {
    this.configurationClass = configurationClass;
    this.backendConfigurationClass = backendConfigurationClass;
    this.backendConfigurationMapperClass = backendConfigurationMapperClass;
    this.backendModuleSupplier = backendModuleSupplier;
  }

  public boolean isForConfiguration(BackendImplementation backendImplementation) {
    return configurationClass.isAssignableFrom(backendImplementation.getClass());
  }

  public void setConfiguration(BackendImplementation backendImplementation) {
    this.backendImplementation = backendImplementation;
  }

  @Override
  protected void configure() {
    install(new BackendModule());
    bind(configurationClass).toInstance(configurationClass.cast(backendImplementation));
    bind(backendConfigurationClass).to(backendConfigurationMapperClass);
    install(backendModuleSupplier.get());
    expose(SqlHelper.class);
    expose(SqlInterface.class);
    expose(SchemaUpdater.class);
    expose(DbBackendService.class);
    expose(IdentifierConstraints.class);
    expose(BackendBundleFactory.class);
    expose(BackendTransactionJobFactory.class);
    expose(ReservedIdGenerator.class);
    expose(SnapshotUpdater.class);
  }
}
