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
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import javax.inject.Singleton;

/**
 * First iteration of Disabled Metric Registry. Only metrics created using ToroMetricRegistry class
 * are disabled. Metrics are mocked, but Timer needs an instance of Context that is private.
 *
 */
@Singleton
public class DisabledMetricRegistry extends ToroMetricRegistry {

  private static final Counter mockedCounter = new MockedCounter();
  private static final Meter mockedMeter = new MockedMeter();
  private static final Histogram mockedHistogram = new MockedHistogram(
      new ExponentiallyDecayingReservoir());
  private static final Timer mockedTimer = new MockedTimer();
  @SuppressWarnings("rawtypes")
  private static final SettableGauge mockedGauge = new MockedGauge();

  public Counter counter(MetricName name) {
    return mockedCounter;
  }

  public Meter meter(MetricName name) {
    return mockedMeter;
  }

  public Histogram histogram(MetricName name) {
    return mockedHistogram;
  }

  public Histogram histogram(MetricName name, boolean resetOnSnapshot) {
    return histogram(name);
  }

  public Timer timer(MetricName name) {
    return mockedTimer;
  }

  public Timer timer(MetricName name, boolean resetOnSnapshot) {
    return timer(name);
  }

  @SuppressWarnings("unchecked")
  public <T> SettableGauge<T> gauge(MetricName name) {
    return mockedGauge;
  }

  public boolean remove(MetricName name) {
    boolean removed = remove(name.getMetricName());
    return removed;
  }

  public static class MockedCounter extends Counter {

    public void inc() {
    }

    public void inc(long n) {
    }

    public void dec() {
    }

    public void dec(long n) {
    }

    @Override
    public long getCount() {
      return 0;
    }
  }

  public static class MockedMeter extends Meter {

    public void mark() {
    }

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

  public static class MockedHistogram extends Histogram {

    public MockedHistogram(Reservoir reservoir) {
      super(reservoir);
    }

    public void update(int value) {
    }

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

  public static class MockedTimer extends Timer {

    private static final Timer shared = new Timer();

    public MockedTimer() {
    }

    public MockedTimer(Reservoir reservoir) {
    }

    public MockedTimer(Reservoir reservoir, Clock clock) {
    }

    public void update(long duration, TimeUnit unit) {
    }

    public <T> T time(Callable<T> event) throws Exception {
      return event.call();
    }

    public Context time() {
      return shared.time();
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

  public static class MockedGauge<T> extends SettableGauge<T> {

    @Override
    public T getValue() {
      return null;
    }

    @Override
    public void setValue(T value) {
    }
  }
}
