package com.torodb.core.metrics.mbeans;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

import javax.management.ObjectName;

import com.codahale.metrics.Timer;

public class JmxTimer extends JmxMeter {
	private final Timer metric;
	private final double durationFactor;
	private final String durationUnit;

	JmxTimer(Timer metric, ObjectName objectName, TimeUnit rateUnit, TimeUnit durationUnit) {
		super(metric, objectName, rateUnit);
		this.metric = metric;
		this.durationFactor = 1.0 / durationUnit.toNanos(1);
		this.durationUnit = durationUnit.toString().toLowerCase(Locale.US);
	}

	public double get50thPercentile() {
		return metric.getSnapshot().getMedian() * durationFactor;
	}

	public double getMin() {
		return metric.getSnapshot().getMin() * durationFactor;
	}

	public double getMax() {
		return metric.getSnapshot().getMax() * durationFactor;
	}

	public double getMean() {
		return metric.getSnapshot().getMean() * durationFactor;
	}

	public double getStdDev() {
		return metric.getSnapshot().getStdDev() * durationFactor;
	}

	public double get75thPercentile() {
		return metric.getSnapshot().get75thPercentile() * durationFactor;
	}

	public double get95thPercentile() {
		return metric.getSnapshot().get95thPercentile() * durationFactor;
	}

	public double get98thPercentile() {
		return metric.getSnapshot().get98thPercentile() * durationFactor;
	}

	public double get99thPercentile() {
		return metric.getSnapshot().get99thPercentile() * durationFactor;
	}

	public double get999thPercentile() {
		return metric.getSnapshot().get999thPercentile() * durationFactor;
	}

	public long[] values() {
		return metric.getSnapshot().getValues();
	}

	public String getDurationUnit() {
		return durationUnit;
	}
}