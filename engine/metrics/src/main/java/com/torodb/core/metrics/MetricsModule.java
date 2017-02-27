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

package com.torodb.core.metrics;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.Singleton;
import com.torodb.core.guice.EssentialToroModule;

/**
 * A module that binds metrics related stuff (specially {@link AdaptorMetricRegistry}).
 */
public class MetricsModule extends EssentialToroModule {

  private MetricsConfig config;

  public MetricsModule(MetricsConfig config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    exposeEssential(MetricNameFactory.class);
    exposeEssential(ToroMetricRegistry.class);

    if (!config.getMetricsEnabled()) {
      bind(DisabledMetricRegistry.class)
          .in(Singleton.class);
      bindEssential(ToroMetricRegistry.class)
          .to(DisabledMetricRegistry.class);
    } else {
      bind(MetricRegistry.class)
          .toInstance(new MetricRegistry());
      bind(AdaptorMetricRegistry.class)
          .in(Singleton.class);
      bindEssential(ToroMetricRegistry.class)
          .to(AdaptorMetricRegistry.class);
    }

    bindEssential(MetricNameFactory.class)
        .to(RootMetricNameFactory.class)
        .in(Singleton.class);
  }

}
