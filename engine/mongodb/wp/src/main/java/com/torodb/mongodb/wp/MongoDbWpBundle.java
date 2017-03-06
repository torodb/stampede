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

package com.torodb.mongodb.wp;

import com.eightkdata.mongowp.server.wp.NettyMongoServer;
import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import com.torodb.core.bundle.AbstractBundle;
import com.torodb.mongodb.core.MongoDbCoreBundle;
import com.torodb.mongodb.wp.guice.MongoDbWpModule;

import java.util.Collection;
import java.util.Collections;

public class MongoDbWpBundle extends AbstractBundle<MongoDbWpExtInt> {
  private final MongoDbCoreBundle coreBundle;
  private final NettyMongoServer nettyMongoServer;

  public MongoDbWpBundle(MongoDbWpConfig config) {
    super(config);

    Injector injector = config.getEssentialInjector().createChildInjector(
        new MongoDbWpModule(
            config.getCoreBundle(),
            config.getPort()
        ));
    this.nettyMongoServer = injector.getInstance(NettyMongoServer.class);
    this.coreBundle = config.getCoreBundle();
  }

  @Override
  protected void postDependenciesStartUp() throws Exception {
    nettyMongoServer.startAsync();
    nettyMongoServer.awaitRunning();
  }

  @Override
  protected void preDependenciesShutDown() throws Exception {
    nettyMongoServer.stopAsync();
    nettyMongoServer.awaitTerminated();
  }

  @Override
  public Collection<Service> getDependencies() {
    return Collections.singleton(coreBundle);
  }

  @Override
  public MongoDbWpExtInt getExternalInterface() {
    return new MongoDbWpExtInt();
  }
}
