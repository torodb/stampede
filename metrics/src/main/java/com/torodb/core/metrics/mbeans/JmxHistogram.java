package com.torodb.core.metrics.mbeans;

import javax.management.ObjectName;

import com.codahale.metrics.Histogram;

public class JmxHistogram extends AbstractBean {
	private final Histogram metric;

	JmxHistogram(Histogram metric, ObjectName objectName) {
		super(objectName);
		this.metric = metric;
	}

	public double get50thPercentile() {
		return metric.getSnapshot().getMedian();
	}

	public long getCount() {
		return metric.getCount();
	}

	public long getMin() {
		return metric.getSnapshot().getMin();
	}

	public long getMax() {
		return metric.getSnapshot().getMax();
	}

	public double getMean() {
		return metric.getSnapshot().getMean();
	}

	public double getStdDev() {
		return metric.getSnapshot().getStdDev();
	}

	public double get75thPercentile() {
		return metric.getSnapshot().get75thPercentile();
	}

	public double get95thPercentile() {
		return metric.getSnapshot().get95thPercentile();
	}

	public double get98thPercentile() {
		return metric.getSnapshot().get98thPercentile();
	}

	public double get99thPercentile() {
		return metric.getSnapshot().get99thPercentile();
	}

	public double get999thPercentile() {
		return metric.getSnapshot().get999thPercentile();
	}

	public long[] values() {
		return metric.getSnapshot().getValues();
	}
}