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

package com.torodb.concurrent.guice;

import com.google.inject.PrivateModule;
import com.google.inject.Singleton;
import com.torodb.concurrent.DefaultConcurrentToolsFactory;
import com.torodb.concurrent.ExecutorServiceShutdownHelper;
import com.torodb.core.concurrent.ConcurrentToolsFactory;

/**
 *
 */
public class ConcurrentModule extends PrivateModule {

  @Override
  protected void configure() {
    bind(ConcurrentToolsFactory.class)
        .to(DefaultConcurrentToolsFactory.class)
        .in(Singleton.class);
    expose(ConcurrentToolsFactory.class);

    bind(ExecutorServiceShutdownHelper.class);
    expose(ExecutorServiceShutdownHelper.class);
  }

}
