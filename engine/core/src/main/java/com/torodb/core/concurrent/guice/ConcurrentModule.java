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

package com.torodb.core.concurrent.guice;

import com.google.inject.Exposed;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.torodb.core.Shutdowner;
import com.torodb.core.concurrent.ConcurrentToolsFactory;
import com.torodb.core.concurrent.DefaultConcurrentToolsFactory;
import com.torodb.core.concurrent.ExecutorServiceShutdownHelper;
import com.torodb.core.logging.LoggerFactory;

import java.time.Clock;

/**
 * A module that binds concurrent utility classes (like {@link ConcurrentToolsFactory} and
 * {@link ExecutorServiceShutdownHelper}).
 */
public class ConcurrentModule extends PrivateModule {

  private final LoggerFactory lifecycleLoggingFactory;

  public ConcurrentModule(LoggerFactory lifecycleLoggingFactory) {
    this.lifecycleLoggingFactory = lifecycleLoggingFactory;
  }

  @Override
  protected void configure() {
    bind(ConcurrentToolsFactory.class)
        .to(DefaultConcurrentToolsFactory.class)
        .in(Singleton.class);
    expose(ConcurrentToolsFactory.class);
  }

  @Provides
  @Exposed
  protected ExecutorServiceShutdownHelper createExecutorServiceShutdownHelper(
      Shutdowner shutdowner, Clock clock) {
    return new ExecutorServiceShutdownHelper(shutdowner, clock, lifecycleLoggingFactory);
  }

}
