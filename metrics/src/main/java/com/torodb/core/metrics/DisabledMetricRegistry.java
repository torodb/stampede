package com.torodb.core.metrics;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import javax.inject.Singleton;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

/**
 * First iteration of Disabled Metric Registry.
 * Only metrics created using ToroMetricRegistry class are disabled.
 * Metrics are mocked, but Timer needs an instance of Context that is private.
 *
 */
@Singleton
public class DisabledMetricRegistry extends ToroMetricRegistry {
	
	private static final Counter mockedCounter = new MockedCounter();
	private static final Meter mockedMeter = new MockedMeter();
	private static final Histogram mockedHistogram = new MockedHistogram(new ExponentiallyDecayingReservoir());
	private static final Timer mockedTimer = new MockedTimer();
	
	public Counter counter(MetricName name) {
		return mockedCounter;
	}

	public Meter meter(MetricName name) {
		return mockedMeter;
	}

	public Histogram histogram(MetricName name) {
		return mockedHistogram;
	}

	public Timer timer(MetricName name) {
		return mockedTimer;
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
}
