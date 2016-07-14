package com.torodb.core.metrics;

import javax.inject.Singleton;

import com.codahale.metrics.Counter;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.torodb.core.metrics.mbeans.MetricMBeans;

@Singleton
public class ToroMetricRegistry extends MetricRegistry {

	private final MetricMBeans metricMBeans = new MetricMBeans();
	
	public ToroMetricRegistry() {
		super();
	}

	public Counter counter(MetricName name) {
		Counter counter = counter(name.getMetricName());
		metricMBeans.registerMBean(counter, name.getMBeanName());
		return counter;
	}

	public Meter meter(MetricName name) {
		Meter meter = meter(name.getMetricName());
		metricMBeans.registerMBean(meter, name.getMBeanName());
		return meter;
	}

	public Histogram histogram(MetricName name) {
		Histogram histogram = register(name, new Histogram(new ExponentiallyDecayingReservoir()));
		metricMBeans.registerMBean(histogram, name.getMBeanName());
		return histogram;
	}

	public Timer timer(MetricName name) {
		Timer timer = register(name, new Timer());
		metricMBeans.registerMBean(timer, name.getMBeanName());
		return timer;
	}

	public <T extends Metric> T register(MetricName name, T metric) {
		try {
			register(name.getMetricName(), metric);
			metricMBeans.registerMBean(metric, name.getMBeanName());
			return metric;
		} catch (IllegalArgumentException e) {
			Metric existing = this.getMetrics().get(name.getMetricName());
			return (T) existing;
		}
	}

	public boolean remove(MetricName name) {
		boolean removed = remove(name.getMetricName());
		try {
			metricMBeans.unregisterMBean(name.getMBeanName());
		} catch (Exception ignore) {
		}

		return removed;
	}
	
}
