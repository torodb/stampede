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

import com.google.common.net.HostAndPort;
import com.google.inject.Injector;
import com.torodb.core.backend.BackendBundle;
import com.torodb.core.bundle.BundleConfig;
import com.torodb.core.logging.LoggerFactory;
import com.torodb.mongodb.core.MongoDbCoreBundle;
import com.torodb.mongodb.wp.MongoDbWpBundle;
import org.apache.logging.log4j.Logger;

import java.util.function.BiFunction;
import java.util.function.Function;

public class ServerConfig {

  private final Injector essentialInjector;
  private final Function<BundleConfig, BackendBundle> backendBundleGenerator;
  private final HostAndPort selfHostAndPort;
  @SuppressWarnings("checkstyle:LineLength")
  private final BiFunction<BundleConfig, MongoDbCoreBundle, MongoDbWpBundle> mongoDbWpBundleGenerator;
  private final Logger logger;

  public ServerConfig(Injector essentialInjector,
      Function<BundleConfig, BackendBundle> backendBundleGenerator, HostAndPort selfHostAndPort,
      BiFunction<BundleConfig, MongoDbCoreBundle, MongoDbWpBundle> mongoDbWpBundleGenerator,
      LoggerFactory loggerFactory) {
    this.essentialInjector = essentialInjector;
    this.backendBundleGenerator = backendBundleGenerator;
    this.selfHostAndPort = selfHostAndPort;
    this.mongoDbWpBundleGenerator = mongoDbWpBundleGenerator;
    this.logger = loggerFactory.apply(ServerService.class);
  }
 
  public Injector getEssentialInjector() {
    return essentialInjector;
  }

  public Logger getLifecycleLogger() {
    return logger;
  }

  public Function<BundleConfig, BackendBundle> getBackendBundleGenerator() {
    return backendBundleGenerator;
  }

  public HostAndPort getSelfHostAndPort() {
    return selfHostAndPort;
  }

  @SuppressWarnings("checkstyle:LineLength")
  public BiFunction<BundleConfig, MongoDbCoreBundle, MongoDbWpBundle> getMongoDbWpBundleGenerator() {
    return mongoDbWpBundleGenerator;
  }

}
