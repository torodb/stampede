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

import com.eightkdata.mongowp.server.api.CommandLibrary;
import com.google.inject.PrivateModule;
import com.google.inject.Singleton;
import com.torodb.mongodb.commands.CommandClassifier;
import com.torodb.mongodb.core.MongoDbCoreConfig;
import com.torodb.mongodb.core.MongodMetrics;
import com.torodb.mongodb.core.MongodServer;
import com.torodb.mongodb.language.ObjectIdFactory;
import com.torodb.torod.TorodServer;

public class MongodCoreModule extends PrivateModule {

  private final MongoDbCoreConfig config;

  public MongodCoreModule(MongoDbCoreConfig config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    expose(MongodServer.class);
    
    bind(MongodServer.class)
        .in(Singleton.class);

    bind(CommandLibrary.class)
        .toInstance(config.getCommandsLibrary());
    bind(CommandClassifier.class)
        .toInstance(config.getCommandClassifier());
    bind(TorodServer.class)
        .toInstance(config.getTorodBundle().getExternalInterface().getTorodServer());

    bind(ObjectIdFactory.class)
        .in(Singleton.class);

    bind(MongodMetrics.class)
        .in(Singleton.class);
  }
}
