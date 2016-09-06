package com.torodb.core.metrics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.inject.Provider;
import javax.inject.Singleton;
import javax.management.ObjectName;

import com.codahale.metrics.Counter;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ObjectNameFactory;
import com.codahale.metrics.Timer;

@Singleton
public class ToroMetricRegistry extends MetricRegistry  {
	
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
		Histogram histogram = register(name, new Histogram(new ExponentiallyDecayingReservoir()));
		return histogram;
	}

    public Timer timer(MetricName name) {
        Timer timer = register(name, new Timer());
        return timer;
    }

    public <T> SettableGauge<T> gauge(MetricName name) {
        SettableGauge<T> gauge = register(name, new SettableGauge<T>());
        return gauge;
    }

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
	
	private static class MbeanNameFactory implements ObjectNameFactory{
		
		private Map<String,ObjectName> names = new ConcurrentHashMap<>();
		
		private void registerName(MetricName name){
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
