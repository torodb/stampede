package com.torodb.core.metrics.mbeans;

import javax.management.ObjectName;

import com.codahale.metrics.Counter;

public class JmxCounter extends AbstractBean {
	private final Counter metric;

	JmxCounter(Counter metric, ObjectName objectName) {
		super(objectName);
		this.metric = metric;
	}

	public long getCount() {
		return metric.getCount();
	}
}