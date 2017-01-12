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
import com.google.inject.Injector;
import com.torodb.core.BuildProperties;
import com.torodb.core.modules.BundleConfig;
import com.torodb.core.modules.BundleConfigImpl;
import com.torodb.core.supervision.Supervisor;
import com.torodb.mongodb.commands.CommandClassifier;
import com.torodb.mongodb.commands.TorodbCommandsLibraryFactory;
import com.torodb.mongodb.commands.impl.CommandClassifierImpl;
import com.torodb.torod.TorodBundle;

import java.time.Clock;

import javax.inject.Inject;

/**
 * The configuration used by {@link MongoDbCoreBundle}.
 */
public class MongoDbCoreConfig extends BundleConfigImpl {
  private final TorodBundle torodBundle;
  private final CommandLibrary commandsLibrary;
  private final CommandClassifier commandClassifier;

  @Inject
  public MongoDbCoreConfig(TorodBundle torodBundle, CommandLibrary commandsLibrary,
      CommandClassifier commandClassifier, Injector essentialInjector, Supervisor supervisor) {
    super(essentialInjector, supervisor);
    this.torodBundle = torodBundle;
    this.commandsLibrary = commandsLibrary;
    this.commandClassifier = commandClassifier;
  }

  /**
   * Creates a configuration that uses several default values.
   */
  public static MongoDbCoreConfig simpleConfig(TorodBundle torodBundle,
      MongodServerConfig mongodServerConfig, BundleConfig bundleConfig) {
    Clock clock = bundleConfig.getEssentialInjector()
        .getInstance(Clock.class);
    BuildProperties buildProp = bundleConfig.getEssentialInjector()
        .getInstance(BuildProperties.class);
    CommandClassifier commandClassifier = CommandClassifierImpl.createDefault(clock, buildProp,
        mongodServerConfig);
    
    return new MongoDbCoreConfig(
        torodBundle,
        TorodbCommandsLibraryFactory.get(buildProp, commandClassifier),
        commandClassifier,
        bundleConfig.getEssentialInjector(),
        bundleConfig.getSupervisor()
    );
  }

  public TorodBundle getTorodBundle() {
    return torodBundle;
  }

  public CommandLibrary getCommandsLibrary() {
    return commandsLibrary;
  }

  public CommandClassifier getCommandClassifier() {
    return commandClassifier;
  }

}
