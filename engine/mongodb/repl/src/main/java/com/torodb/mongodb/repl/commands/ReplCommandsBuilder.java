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

package com.torodb.mongodb.repl.commands;

import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.torodb.core.modules.BundleConfig;
import com.torodb.core.supervision.Supervisor;
import com.torodb.mongodb.filters.DatabaseFilter;
import com.torodb.mongodb.filters.IndexFilter;
import com.torodb.mongodb.filters.NamespaceFilter;
import com.torodb.mongodb.repl.ReplicationFilters;
import com.torodb.mongodb.repl.commands.impl.CommandFilterUtil;
import com.torodb.mongodb.repl.guice.MongoDbRepl;

/**
 * A utility class used to generate {@link ReplCommandExecutor} and {@link ReplCommandLibrary}.
 */
public class ReplCommandsBuilder {

  private final ReplCommandLibrary replCommandsLibrary;
  private final ReplCommandExecutor replCommandsExecutor;

  public ReplCommandsBuilder(BundleConfig generalConfig, ReplicationFilters replFilters) {
    Injector replCommandsInjector = generalConfig.getEssentialInjector()
        .createChildInjector(
            new ExtraModule(generalConfig, replFilters),
            new ReplCommandsGuiceModule()
        );

    replCommandsLibrary = replCommandsInjector.getInstance(ReplCommandLibrary.class);
    replCommandsExecutor = replCommandsInjector.getInstance(ReplCommandExecutor.class);
  }

  public ReplCommandLibrary getReplCommandsLibrary() {
    return replCommandsLibrary;
  }

  public ReplCommandExecutor getReplCommandsExecutor() {
    return replCommandsExecutor;
  }

  private static final class ExtraModule extends AbstractModule {
    private final BundleConfig generalConfig;
    private final ReplicationFilters replFilters;

    public ExtraModule(BundleConfig generalConfig, ReplicationFilters replFilters) {
      this.generalConfig = generalConfig;
      this.replFilters = replFilters;
    }

    @Override
    protected void configure() {
      bind(CommandFilterUtil.class);
      bind(DatabaseFilter.class)
          .toInstance(replFilters.getDatabaseFilter());
      bind(NamespaceFilter.class)
          .toInstance(replFilters.getNamespaceFilter());
      bind(IndexFilter.class)
          .toInstance(replFilters.getIndexFilter());
      bind(Supervisor.class)
          .annotatedWith(MongoDbRepl.class)
          .toInstance(generalConfig.getSupervisor());
    }
  }

}
