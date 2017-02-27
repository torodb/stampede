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


public interface ToroMetricRegistry {

  public Counter counter(MetricName name);

  public Meter meter(MetricName name);

  public default Histogram histogram(MetricName name) {
    return histogram(name, false);
  }

  /**
   *
   * @param name
   * @param resetOnSnapshot This is usually true if you're using snapshots as a means of defining
   *                        the window in which you want to calculate, say, the 99.9th percentile
   * @return
   */
  public Histogram histogram(MetricName name, boolean resetOnSnapshot);

  public default Timer timer(MetricName name) {
    return timer(name, false);
  }

  /**
   *
   * @param name
   * @param resetOnSnapshot This is usually true if you're using snapshots as a means of defining
   *                        the window in which you want to calculate, say, the 99.9th percentile
   * @return
   */
  public Timer timer(MetricName name, boolean resetOnSnapshot);

  public <T> SettableGauge<T> gauge(MetricName name);

  public <T extends Metric> T register(MetricName name, T metric);

  public boolean remove(MetricName name);
}
