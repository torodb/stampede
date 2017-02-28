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
import com.codahale.metrics.Timer;
import com.torodb.core.metrics.directory.Directory;

public class DirectoryToroMetricRegistry implements ToroMetricRegistry {
  
  private final Directory directory;
  private final SafeRegistry safeMetricRegistry;

  public DirectoryToroMetricRegistry(Directory directory, SafeRegistry safeMetricRegistry) {
    this.directory = directory;
    this.safeMetricRegistry = safeMetricRegistry;
  }

  @Override
  public ToroMetricRegistry createSubRegistry(String key, String value) {
    return new DirectoryToroMetricRegistry(
        directory.createDirectory(key, value),
        safeMetricRegistry
    );
  }

  @Override
  public ToroMetricRegistry createSubRegistry(String middleName) {
    return new DirectoryToroMetricRegistry(
        directory.createDirectory(middleName),
        safeMetricRegistry
    );
  }

  @Override
  public Counter counter(String finalName) {
    return safeMetricRegistry.counter(directory.createName(finalName));
  }

  @Override
  public Meter meter(String finalName) {
    return safeMetricRegistry.meter(directory.createName(finalName));
  }

  @Override
  public Histogram histogram(String finalName, boolean resetOnSnapshot) {
    return safeMetricRegistry.histogram(directory.createName(finalName), resetOnSnapshot);
  }

  @Override
  public Timer timer(String finalName, boolean resetOnSnapshot) {
    return safeMetricRegistry.timer(directory.createName(finalName), resetOnSnapshot);
  }

  @Override
  public <T> SettableGauge<T> gauge(String finalName) {
    return safeMetricRegistry.gauge(directory.createName(finalName));
  }

  @Override
  public <T extends Metric> T register(String finalName, T metric) {
    return safeMetricRegistry.register(directory.createName(finalName), metric);
  }

  @Override
  public boolean remove(String finalName) {
    return safeMetricRegistry.remove(directory.createName(finalName));
  }

}
