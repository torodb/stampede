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

import com.google.inject.Injector;
import com.torodb.core.backend.BackendBundle;
import com.torodb.core.modules.BundleConfig;
import com.torodb.mongodb.wp.MongoDbWpBundle;
import com.torodb.torod.TorodBundle;

import java.util.function.BiFunction;
import java.util.function.Function;

public class ServerConfig {

  private final Injector essentialInjector;
  private final Function<BundleConfig, BackendBundle> backendBundleGenerator;
  private final BiFunction<BundleConfig, TorodBundle, MongoDbWpBundle> mongoDbWpBundleGenerator;

  public ServerConfig(Injector essentialInjector,
      Function<BundleConfig, BackendBundle> backendBundleGenerator,
      BiFunction<BundleConfig, TorodBundle, MongoDbWpBundle> wpBundleGenerator) {
    this.essentialInjector = essentialInjector;
    this.backendBundleGenerator = backendBundleGenerator;
    this.mongoDbWpBundleGenerator = wpBundleGenerator;
  }

  public Injector getEssentialInjector() {
    return essentialInjector;
  }

  public Function<BundleConfig, BackendBundle> getBackendBundleGenerator() {
    return backendBundleGenerator;
  }

  public BiFunction<BundleConfig, TorodBundle, MongoDbWpBundle> getMongoDbWpBundleGenerator() {
    return mongoDbWpBundleGenerator;
  }

}
