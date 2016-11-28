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

package com.torodb.standalone;

import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.torodb.standalone.config.model.Config;

import java.time.Clock;
import java.util.concurrent.ThreadFactory;

/**
 *
 */
public class ToroDbBootstrap {

  private ToroDbBootstrap() {
  }

  public static Service createStandaloneService(Config config, Clock clock) {
    Injector bootstrapInjector = Guice.createInjector(new BootstrapModule(
        config, clock));
    ThreadFactory threadFactory = bootstrapInjector.getInstance(
        ThreadFactory.class);

    return new ToroDbService(threadFactory, bootstrapInjector);
  }

}
