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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Timer;
import com.google.inject.Inject;
import com.torodb.core.metrics.SettableGauge;
import org.mpierce.metrics.reservoir.hdrhistogram.HdrHistogramReservoir;
import org.mpierce.metrics.reservoir.hdrhistogram.HdrHistogramResetOnSnapshotReservoir;


/**
 * An adaptor whose external interface is a {@link SafeRegistry} and
 * the internal is a {@link com.codahale.metrics.MetricRegistry Codahale Metric Registry}.
 */
class AdaptorSafeRegistry implements SafeRegistry {

  private final com.codahale.metrics.MetricRegistry adapted;

  @Inject
  public AdaptorSafeRegistry(com.codahale.metrics.MetricRegistry adapted) {
    this.adapted = adapted;
  }

  @Override
  public Counter counter(Name name) {
    Counter counter = adapted.counter(name.getQualifiedName());
    return counter;
  }

  @Override
  public Meter meter(Name name) {
    Meter meter = adapted.meter(name.getQualifiedName());
    return meter;
  }

  /**
   * @param resetOnSnapshot This is usually true if you're using snapshots as a means of defining
   *                        the window in which you want to calculate, say, the 99.9th percentile
   */
  @Override
  public Histogram histogram(Name name, boolean resetOnSnapshot) {
    Reservoir reservoir;
    if (resetOnSnapshot) {
      reservoir = new HdrHistogramResetOnSnapshotReservoir();
    } else {
      reservoir = new HdrHistogramReservoir();
    }
    Histogram histogram = register(name, new Histogram(reservoir));
    return histogram;
  }

  /**
   * @param resetOnSnapshot This is usually true if you're using snapshots as a means of defining
   *                        the window in which you want to calculate, say, the 99.9th percentile
   */
  @Override
  public Timer timer(Name name, boolean resetOnSnapshot) {
    Reservoir reservoir;
    if (resetOnSnapshot) {
      reservoir = new HdrHistogramResetOnSnapshotReservoir();
    } else {
      reservoir = new HdrHistogramReservoir();
    }
    Timer timer = register(name, new Timer(reservoir));
    return timer;
  }

  @Override
  public <T> SettableGauge<T> gauge(Name name) {
    SettableGauge<T> gauge = register(name, new SettableGauge<T>());
    return gauge;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends Metric> T register(Name name, T metric) {
    try {
      adapted.register(name.getQualifiedName(), metric);
      return metric;
    } catch (IllegalArgumentException e) {
      Metric existing = adapted.getMetrics().get(name.getQualifiedName());
      return (T) existing;
    }
  }

  @Override
  public boolean remove(Name name) {
    boolean removed = adapted.remove(name.getQualifiedName());
    return removed;
  }

}
