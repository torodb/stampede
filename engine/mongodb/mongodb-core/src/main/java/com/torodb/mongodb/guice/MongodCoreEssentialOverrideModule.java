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

import com.torodb.core.guice.EssentialToDefaultModule;
import com.torodb.core.logging.LoggerFactory;
import com.torodb.core.metrics.ToroMetricRegistry;

import java.util.Optional;

public class MongodCoreEssentialOverrideModule extends EssentialToDefaultModule {

  private final Optional<ToroMetricRegistry> toroMetricRegistry;
  private final LoggerFactory loggerFactory;

  public MongodCoreEssentialOverrideModule(Optional<ToroMetricRegistry> toroMetricRegistry,
      LoggerFactory loggerFactory) {
    this.toroMetricRegistry = toroMetricRegistry;
    this.loggerFactory = loggerFactory;
  }

  @Override
  protected void bindToroMetricRegistry() {
    if (toroMetricRegistry.isPresent()) {
      bind(ToroMetricRegistry.class)
          .toInstance(toroMetricRegistry.get());
    } else {
      super.bindToroMetricRegistry();
    }
  }

  @Override
  protected void bindLoggerFactory() {
    bind(LoggerFactory.class)
        .toInstance(loggerFactory);
  }

}
