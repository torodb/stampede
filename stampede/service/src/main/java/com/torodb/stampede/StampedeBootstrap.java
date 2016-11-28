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

import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.torodb.stampede.config.model.Config;

import java.time.Clock;
import java.util.concurrent.ThreadFactory;

/**
 *
 */
public class StampedeBootstrap {

  private StampedeBootstrap() {
  }

  public static Service createStampedeService(Config config, Clock clock) {
    return createStampedeService(new BootstrapModule(
        config, clock));
  }

  public static Service createStampedeService(BootstrapModule bootstrapModule) {
    Injector bootstrapInjector = Guice.createInjector(bootstrapModule);
    ThreadFactory threadFactory = bootstrapInjector.getInstance(
        ThreadFactory.class);

    return new StampedeService(threadFactory, bootstrapInjector);
  }

}
