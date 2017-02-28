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

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public interface ToroMetricRegistry {

  public ToroMetricRegistry createSubRegistry(String key, String midleName);

  /**
   * Creates a new {@link ToroMetricRegistry} with a default key.
   *
   * <p/>It is the same as to call {@link #createSubRegistry(java.lang.String, java.lang.String) }
   * with a default key name defined by this object.
   */
  public ToroMetricRegistry createSubRegistry(String middleName);

  public Counter counter(String finalName);

  public Meter meter(String finalName);

  public default Histogram histogram(String finalName) {
    return histogram(finalName, false);
  }

  /**
   * @param resetOnSnapshot This is usually true if you're using snapshots as a means of defining
   *                        the window in which you want to calculate, say, the 99.9th percentile
   */
  public Histogram histogram(String finalName, boolean resetOnSnapshot);

  public default Timer timer(String finalName) {
    return timer(finalName, false);
  }

  /**
   * @param resetOnSnapshot This is usually true if you're using snapshots as a means of defining
   *                        the window in which you want to calculate, say, the 99.9th percentile
   */
  public Timer timer(String finalName, boolean resetOnSnapshot);

  public <T> SettableGauge<T> gauge(String finalName);

  public <T extends Metric> T register(String finalName, T metric);

  public boolean remove(String finalName);

}
