package com.torodb.core.metrics.mbeans;

import javax.management.ObjectName;

import com.codahale.metrics.Gauge;

public class JmxGauge extends AbstractBean {
	
	private final Gauge<?> metric;

	JmxGauge(Gauge<?> metric, ObjectName objectName) {
		super(objectName);
		this.metric = metric;
	}

	public Object getValue() {
		return metric.getValue();
	}
}