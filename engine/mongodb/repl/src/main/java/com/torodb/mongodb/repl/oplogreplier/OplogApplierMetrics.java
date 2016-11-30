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

package com.torodb.mongodb.repl.oplogreplier;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.torodb.core.metrics.MetricNameFactory;
import com.torodb.core.metrics.ToroMetricRegistry;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

/**
 *
 */
@ThreadSafe
public class OplogApplierMetrics {

  private final Histogram maxDelay;
  private final Meter applied;
  private final Histogram batchSize;
  private final Histogram applicationCost;

  @Inject
  public OplogApplierMetrics(ToroMetricRegistry registry) {
    MetricNameFactory factory = new MetricNameFactory("OplogApplier");

    maxDelay = registry.histogram(factory.createMetricName("maxDelay"));
    registry.gauge(factory.createMetricName("maxDelayUnits")).setValue("milliseconds");

    applied = registry.meter(factory.createMetricName("applied"));
    registry.gauge(factory.createMetricName("appliedUnit")).setValue("ops");

    batchSize = registry.histogram(factory.createMetricName("batchSize"));
    registry.gauge(factory.createMetricName("batchSizeUnit")).setValue("ops/batch");

    applicationCost = registry.histogram(factory.createMetricName("applicationCost"));
    registry.gauge(factory.createMetricName("applicationCostUnit")).setValue("microseconds/op");
  }

  public Histogram getMaxDelay() {
    return maxDelay;
  }

  public Meter getApplied() {
    return applied;
  }

  public Histogram getBatchSize() {
    return batchSize;
  }

  public Histogram getApplicationCost() {
    return applicationCost;
  }
}
