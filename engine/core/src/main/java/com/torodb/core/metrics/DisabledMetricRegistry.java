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

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import javax.inject.Singleton;

/**
 * First iteration of Disabled Metric Registry.
 *
 * Only metrics created using {@link ToroMetricRegistry} class are disabled. Metrics are mocked, but
 * Timer needs an instance of Context that is private.
 */
@Singleton
public class DisabledMetricRegistry implements ToroMetricRegistry {

  private static final Counter MOCKED_COUNTER = new MockedCounter();
  private static final Meter MOCKED_METER = new MockedMeter();
  private static final Histogram MOCKED_HISTOGRAM = new MockedHistogram(
      new ExponentiallyDecayingReservoir());
  private static final Timer MOCKED_TIMER = new MockedTimer();
  @SuppressWarnings("rawtypes")
  private static final SettableGauge MOCKED_GAUGE = new MockedGauge();

  @Override
  public ToroMetricRegistry createSubRegistry(String key, String midleName) {
    return this;
  }

  @Override
  public ToroMetricRegistry createSubRegistry(String middleName) {
    return this;
  }

  @Override
  public Counter counter(String name) {
    return MOCKED_COUNTER;
  }

  @Override
  public Meter meter(String name) {
    return MOCKED_METER;
  }

  @Override
  public Histogram histogram(String name, boolean resetOnSnapshot) {
    return MOCKED_HISTOGRAM;
  }

  @Override
  public Timer timer(String name, boolean resetOnSnapshot) {
    return MOCKED_TIMER;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> SettableGauge<T> gauge(String name) {
    return MOCKED_GAUGE;
  }

  @Override
  public boolean remove(String name) {
    return true;
  }

  @Override
  public <T extends Metric> T register(String name, T metric) {
    return metric;
  }

  private static class MockedCounter extends Counter {

    @Override
    public void inc() {
    }

    @Override
    public void inc(long n) {
    }

    @Override
    public void dec() {
    }

    @Override
    public void dec(long n) {
    }

    @Override
    public long getCount() {
      return 0;
    }
  }

  private static class MockedMeter extends Meter {

    @Override
    public void mark() {
    }

    @Override
    public void mark(long n) {
    }

    @Override
    public long getCount() {
      return 0;
    }

    @Override
    public double getFifteenMinuteRate() {
      return 0;
    }

    @Override
    public double getFiveMinuteRate() {
      return 0;
    }

    @Override
    public double getMeanRate() {
      return 0;
    }

    @Override
    public double getOneMinuteRate() {
      return 0;
    }
  }

  private static class MockedHistogram extends Histogram {

    public MockedHistogram(Reservoir reservoir) {
      super(reservoir);
    }

    @Override
    public void update(int value) {
    }

    @Override
    public void update(long value) {
    }

    @Override
    public long getCount() {
      return 0;
    }

    @Override
    public Snapshot getSnapshot() {
      return super.getSnapshot();
    }
  }

  private static class MockedTimer extends Timer {

    private static final Timer SHARED = new Timer();

    public MockedTimer() {
    }

    public MockedTimer(Reservoir reservoir) {
    }

    public MockedTimer(Reservoir reservoir, Clock clock) {
    }

    @Override
    public void update(long duration, TimeUnit unit) {
    }

    @Override
    public <T> T time(Callable<T> event) throws Exception {
      return event.call();
    }

    @Override
    public Context time() {
      return SHARED.time();
    }

    @Override
    public long getCount() {
      return 0;
    }

    @Override
    public double getFifteenMinuteRate() {
      return 0;
    }

    @Override
    public double getFiveMinuteRate() {
      return 0;
    }

    @Override
    public double getMeanRate() {
      return 0;
    }

    @Override
    public double getOneMinuteRate() {
      return 0;
    }

    @Override
    public Snapshot getSnapshot() {
      return null;
    }
  }

  private static class MockedGauge<T> extends SettableGauge<T> {

    @Override
    public T getValue() {
      return null;
    }

    @Override
    public void setValue(T value) {
    }
  }
}
