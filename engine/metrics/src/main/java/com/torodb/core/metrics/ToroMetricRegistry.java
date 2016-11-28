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
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ObjectNameFactory;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Timer;
import org.mpierce.metrics.reservoir.hdrhistogram.HdrHistogramReservoir;
import org.mpierce.metrics.reservoir.hdrhistogram.HdrHistogramResetOnSnapshotReservoir;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.ObjectName;

public class ToroMetricRegistry extends MetricRegistry {

  private final MbeanNameFactory mbeanNameFactory = new MbeanNameFactory();

  public ToroMetricRegistry() {
    super();
    final JmxReporter reporter = JmxReporter
        .forRegistry(this)
        .createsObjectNamesWith(mbeanNameFactory)
        .build();
    reporter.start();
  }

  public Counter counter(MetricName name) {
    mbeanNameFactory.registerName(name);
    Counter counter = counter(name.getMetricName());
    return counter;
  }

  public Meter meter(MetricName name) {
    mbeanNameFactory.registerName(name);
    Meter meter = meter(name.getMetricName());
    return meter;
  }

  public Histogram histogram(MetricName name) {
    Histogram histogram = register(name, new Histogram(new HdrHistogramReservoir()));
    return histogram;
  }

  /**
   *
   * @param name
   * @param resetOnSnapshot This is usually true if you're using snapshots as a means of defining
   *                        the window in which you want to calculate, say, the 99.9th percentile
   * @return
   */
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

  public Timer timer(MetricName name) {
    Timer timer = register(name, new Timer(new HdrHistogramReservoir()));
    return timer;
  }

  /**
   *
   * @param name
   * @param resetOnSnapshot This is usually true if you're using snapshots as a means of defining
   *                        the window in which you want to calculate, say, the 99.9th percentile
   * @return
   */
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

  public <T> SettableGauge<T> gauge(MetricName name) {
    SettableGauge<T> gauge = register(name, new SettableGauge<T>());
    return gauge;
  }

  @SuppressWarnings("unchecked")
  public <T extends Metric> T register(MetricName name, T metric) {
    mbeanNameFactory.registerName(name);
    try {
      register(name.getMetricName(), metric);
      return metric;
    } catch (IllegalArgumentException e) {
      Metric existing = this.getMetrics().get(name.getMetricName());
      return (T) existing;
    }
  }

  public boolean remove(MetricName name) {
    boolean removed = remove(name.getMetricName());
    return removed;
  }

  private static class MbeanNameFactory implements ObjectNameFactory {

    private Map<String, ObjectName> names = new ConcurrentHashMap<>();

    private void registerName(MetricName name) {
      names.put(name.getMetricName(), name.getMBeanName());
    }

    @Override
    public ObjectName createName(String type, String domain, String name) {
      return names.computeIfAbsent(name, n -> {
        try {
          return new ObjectName(domain, type, name);
        } catch (Exception e) {
          return null;
        }
      });
    }
  }

}
