package com.torodb.core.metrics;

public class MetricNameFactory {

	public static final String GROUP_NAME = "com.torodb.metrics";

	private final String type;

	public MetricNameFactory(String type) {
		this.type = type;
	}

	public MetricName createMetricName(String metricName) {
		return createMetricName(GROUP_NAME, type, metricName);
	}

	public static MetricName createMetricName(String group, String type, String metricName) {
		return new MetricName(group, type, metricName);
	}

}
