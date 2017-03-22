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
import com.torodb.core.metrics.SettableGauge;
import com.torodb.core.metrics.SettableGauge;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A class where metrics are registered.
 *
 * <p/>It is simmilar to {@link MetricRegistry Codahale Metric Registry}, but recives
 * {@link Name metric names} instead of plain strings as argument.
 */
@ThreadSafe
public interface SafeRegistry {

  public Counter counter(Name name);

  public Meter meter(Name name);

  public default Histogram histogram(Name name) {
    return histogram(name, false);
  }

  /**
   * @param resetOnSnapshot This is usually true if you're using snapshots as a means of defining
   *                        the window in which you want to calculate, say, the 99.9th percentile
   */
  public Histogram histogram(Name name, boolean resetOnSnapshot);

  public default Timer timer(Name name) {
    return timer(name, false);
  }

  /**
   * @param resetOnSnapshot This is usually true if you're using snapshots as a means of defining
   *                        the window in which you want to calculate, say, the 99.9th percentile
   */
  public Timer timer(Name name, boolean resetOnSnapshot);

  public <T> SettableGauge<T> gauge(Name name);

  public <T extends Metric> T register(Name name, T metric);

  public boolean remove(Name name);

}
