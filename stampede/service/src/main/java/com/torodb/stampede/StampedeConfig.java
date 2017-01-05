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

import com.google.inject.Injector;
import com.torodb.core.backend.BackendBundle;
import com.torodb.core.modules.BundleConfig;
import com.torodb.mongodb.repl.MongoDbReplConfigBuilder;

import java.util.function.Function;

public class StampedeConfig {

  private final Injector essentialInjector;
  private final Function<BundleConfig, BackendBundle> backendBundleGenerator;
  private final Function<BundleConfig, MongoDbReplConfigBuilder> replBundleConfigBuilderGenerator;

  public StampedeConfig(Injector essentialInjector,
      Function<BundleConfig, BackendBundle> backendBundleGenerator,
      Function<BundleConfig, MongoDbReplConfigBuilder> replBundleConfigBuilder) {
    this.essentialInjector = essentialInjector;
    this.backendBundleGenerator = backendBundleGenerator;
    this.replBundleConfigBuilderGenerator = replBundleConfigBuilder;
  }

  public Injector getEssentialInjector() {
    return essentialInjector;
  }

  /**
   * Returns a function used to create {@link BackendBundle backend bundles} given a generic
   * bundle configuration.
   *
   * <p>This is an abstraction that disjoins specific backend configuration (usually specified on
   * the main module by reading a config file) and the {@link StampedeService} that uses the
   * bundle.
   */
  public Function<BundleConfig, BackendBundle> getBackendBundleGenerator() {
    return backendBundleGenerator;
  }

  /**
   * Returns a function used to create a partial repl config builder given a generic bundle
   * configuration.
   *
   * <p>This is an abstraction that disjoins specific configuration (usually specified on
   * the main module by reading a config file) and the {@link StampedeService} that uses the
   * bundle.
   */
  public Function<BundleConfig, MongoDbReplConfigBuilder> getReplBundleConfigBuilderGenerator() {
    return replBundleConfigBuilderGenerator;
  }
}
