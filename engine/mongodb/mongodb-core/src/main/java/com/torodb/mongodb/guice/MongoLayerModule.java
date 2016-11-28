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

package com.torodb.mongodb.guice;

import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.torodb.mongodb.commands.CommandImplementionsModule;
import com.torodb.mongodb.commands.TorodbCommandsLibrary;
import com.torodb.mongodb.core.MongodMetrics;
import com.torodb.mongodb.core.MongodServer;
import com.torodb.mongodb.language.ObjectIdFactory;
import com.torodb.torod.TorodBundle;
import com.torodb.torod.TorodServer;

/**
 *
 */
public class MongoLayerModule extends PrivateModule {

  private final Module commandImplementationModule;

  public MongoLayerModule() {
    this(new CommandImplementionsModule());
  }

  public MongoLayerModule(Module commandImplementationModule) {
    this.commandImplementationModule = commandImplementationModule;
  }

  @Override
  protected void configure() {
    bind(MongodServer.class)
        .in(Singleton.class);
    expose(MongodServer.class);

    install(commandImplementationModule);

    bind(ObjectIdFactory.class)
        .in(Singleton.class);
    expose(ObjectIdFactory.class);

    bind(TorodbCommandsLibrary.class)
        .in(Singleton.class);
    expose(TorodbCommandsLibrary.class);

    bind(MongodMetrics.class)
        .in(Singleton.class);
  }

  @Provides
  TorodServer getTorodServer(TorodBundle torodBundle) {
    return torodBundle.getTorodServer();
  }
}
