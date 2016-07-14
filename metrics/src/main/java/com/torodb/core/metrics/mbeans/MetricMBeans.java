package com.torodb.core.metrics.mbeans;

import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.Timer;

public class MetricMBeans {

	private final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

	public void registerMBean(Metric metric, ObjectName name) {
		AbstractBean mbean;
		if (metric instanceof Gauge) {
			mbean = new JmxGauge((Gauge<?>) metric, name);
		} else if (metric instanceof Counter) {
			mbean = new JmxCounter((Counter) metric, name);
		} else if (metric instanceof Histogram) {
			mbean = new JmxHistogram((Histogram) metric, name);
		} else if (metric instanceof Meter) {
			mbean = new JmxMeter((Meter) metric, name, TimeUnit.SECONDS);
		} else if (metric instanceof Timer) {
			mbean = new JmxTimer((Timer) metric, name, TimeUnit.SECONDS, TimeUnit.MICROSECONDS);
		} else {
			throw new IllegalArgumentException("Unknown metric type: " + metric.getClass());
		}

		try {
			mBeanServer.registerMBean(mbean, name);
		} catch (Exception ignored) {
		}
	}

	public void unregisterMBean(ObjectName mBeanName) throws MBeanRegistrationException, InstanceNotFoundException {
		mBeanServer.unregisterMBean(mBeanName);
	}

}
