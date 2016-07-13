package com.torodb.core.metrics.mbeans;

import javax.management.ObjectName;

public abstract class AbstractBean implements MetricJmxMBean {
	
	private final ObjectName objectName;

	AbstractBean(ObjectName objectName) {
		this.objectName = objectName;
	}

	@Override
	public ObjectName objectName() {
		return objectName;
	}
}