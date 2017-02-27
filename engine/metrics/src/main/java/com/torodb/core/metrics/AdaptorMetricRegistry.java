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
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.ObjectNameFactory;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Timer;
import com.google.inject.Inject;
import org.mpierce.metrics.reservoir.hdrhistogram.HdrHistogramReservoir;
import org.mpierce.metrics.reservoir.hdrhistogram.HdrHistogramResetOnSnapshotReservoir;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.ObjectName;

/**
 * An adaptor whose external interface is a {@link ToroMetricRegistry ToroDB Metric Registry} and
 * the internal is a {@link com.codahale.metrics.MetricRegistry Codahale Metric Registry}.
 */
class AdaptorMetricRegistry implements ToroMetricRegistry {

  private final com.codahale.metrics.MetricRegistry adapted;
  private final MbeanNameFactory mbeanNameFactory = new MbeanNameFactory();

  @Inject
  public AdaptorMetricRegistry(com.codahale.metrics.MetricRegistry adapted) {
    this.adapted = adapted;
    final JmxReporter reporter = JmxReporter
        .forRegistry(adapted)
        .createsObjectNamesWith(mbeanNameFactory)
        .build();
    reporter.start();
  }

  @Override
  public Counter counter(MetricName name) {
    mbeanNameFactory.registerName(name);
    Counter counter = adapted.counter(name.getMetricName());
    return counter;
  }

  @Override
  public Meter meter(MetricName name) {
    mbeanNameFactory.registerName(name);
    Meter meter = adapted.meter(name.getMetricName());
    return meter;
  }

  /**
   *
   * @param name
   * @param resetOnSnapshot This is usually true if you're using snapshots as a means of defining
   *                        the window in which you want to calculate, say, the 99.9th percentile
   * @return
   */
  @Override
  public Histogram histogram(MetricName name, boolean resetOnSnapshot) {
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
   *
   * @param name
   * @param resetOnSnapshot This is usually true if you're using snapshots as a means of defining
   *                        the window in which you want to calculate, say, the 99.9th percentile
   * @return
   */
  @Override
  public Timer timer(MetricName name, boolean resetOnSnapshot) {
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
  public <T> SettableGauge<T> gauge(MetricName name) {
    SettableGauge<T> gauge = register(name, new SettableGauge<T>());
    return gauge;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends Metric> T register(MetricName name, T metric) {
    mbeanNameFactory.registerName(name);
    try {
      adapted.register(name.getMetricName(), metric);
      return metric;
    } catch (IllegalArgumentException e) {
      Metric existing = adapted.getMetrics().get(name.getMetricName());
      return (T) existing;
    }
  }

  @Override
  public boolean remove(MetricName name) {
    boolean removed = adapted.remove(name.getMetricName());
    return removed;
  }

  private static class MbeanNameFactory implements ObjectNameFactory {

    private Map<String, ObjectName> names = new ConcurrentHashMap<>();

    private void registerName(MetricName name) {
      names.put(name.getMetricName(), name.getMBeanName());
    }

    @Override
    public ObjectName createName(String type, String domain, String mbeanName) {
      return names.computeIfAbsent(mbeanName, n -> {
        try {
          return new ObjectName(domain, type, mbeanName);
        } catch (Exception e) {
          return null;
        }
      });
    }
  }

}
