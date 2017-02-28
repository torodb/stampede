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

package com.torodb.mongodb.core;

import com.eightkdata.mongowp.server.api.CommandLibrary;
import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import com.torodb.core.bundle.AbstractBundle;
import com.torodb.mongodb.guice.MongodCoreModule;
import com.torodb.torod.TorodBundle;

import java.util.Collection;
import java.util.Collections;

public class MongoDbCoreBundle extends AbstractBundle<MongoDbCoreExtInt> {

  private final TorodBundle torodBundle;
  private final MongodServer mongodServer;
  private final CommandLibrary commandLibrary;

  public MongoDbCoreBundle(MongoDbCoreConfig bundleConfig) {
    super(bundleConfig);

    this.torodBundle = bundleConfig.getTorodBundle();
    this.commandLibrary = bundleConfig.getCommandsLibrary();

    Injector injector = bundleConfig.getEssentialInjector().createChildInjector(
        new MongodCoreModule(bundleConfig)
    );

    mongodServer = injector.getInstance(MongodServer.class);
  }

  @Override
  protected void postDependenciesStartUp() throws Exception {
    mongodServer.startAsync();
    mongodServer.awaitRunning();
  }

  @Override
  protected void preDependenciesShutDown() throws Exception {
    mongodServer.stopAsync();
    mongodServer.awaitTerminated();
  }

  @Override
  public Collection<Service> getDependencies() {
    return Collections.singleton(torodBundle);
  }

  @Override
  public MongoDbCoreExtInt getExternalInterface() {
    return new MongoDbCoreExtInt(mongodServer, commandLibrary);
  }

  

}
