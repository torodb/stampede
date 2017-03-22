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
import com.codahale.metrics.Timer;
import com.torodb.core.metrics.SettableGauge;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.ObjectName;

/**
 * A {@link ToroMetricRegistry} that delegates on also externalize the metrics using JMX.
 */
public class MBeanSafeRegistry implements SafeRegistry {

  private final SafeRegistry delegate;
  private final MbeanNameFactory mbeanNameFactory;

  public MBeanSafeRegistry(MetricRegistry codehaleMetricRegistry) {
    this.delegate = new AdaptorSafeRegistry(codehaleMetricRegistry);
    this.mbeanNameFactory = new MbeanNameFactory();
    final JmxReporter reporter = JmxReporter
        .forRegistry(codehaleMetricRegistry)
        .createsObjectNamesWith(mbeanNameFactory)
        .build();
    reporter.start();
  }

  @Override
  public Counter counter(Name name) {
    mbeanNameFactory.registerName(name);
    return delegate.counter(name);
  }

  @Override
  public Meter meter(Name name) {
    mbeanNameFactory.registerName(name);
    return delegate.meter(name);
  }

  @Override
  public Histogram histogram(Name name, boolean resetOnSnapshot) {
    mbeanNameFactory.registerName(name);
    return delegate.histogram(name, resetOnSnapshot);
  }

  @Override
  public Timer timer(Name name, boolean resetOnSnapshot) {
    mbeanNameFactory.registerName(name);
    return delegate.timer(name, resetOnSnapshot);
  }

  @Override
  public <T> SettableGauge<T> gauge(Name name) {
    mbeanNameFactory.registerName(name);
    return delegate.gauge(name);
  }

  @Override
  public <T extends Metric> T register(Name name, T metric) {
    mbeanNameFactory.registerName(name);
    return delegate.register(name, metric);
  }

  @Override
  public boolean remove(Name name) {
    return delegate.remove(name);
  }


  private static class MbeanNameFactory implements ObjectNameFactory {

    private Map<String, ObjectName> names = new ConcurrentHashMap<>();

    private void registerName(Name name) {
      names.put(name.getQualifiedName(), name.getMBeanName());
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
