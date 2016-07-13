package com.torodb.core.metrics.mbeans;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

import javax.management.ObjectName;

import com.codahale.metrics.Metered;

public class JmxMeter extends AbstractBean {
	private final Metered metric;
	private final double rateFactor;
	private final String rateUnit;

	JmxMeter(Metered metric, ObjectName objectName, TimeUnit rateUnit) {
		super(objectName);
		this.metric = metric;
		this.rateFactor = rateUnit.toSeconds(1);
		this.rateUnit = "events/" + calculateRateUnit(rateUnit);
	}

	public long getCount() {
		return metric.getCount();
	}

	public double getMeanRate() {
		return metric.getMeanRate() * rateFactor;
	}

	public double getOneMinuteRate() {
		return metric.getOneMinuteRate() * rateFactor;
	}

	public double getFiveMinuteRate() {
		return metric.getFiveMinuteRate() * rateFactor;
	}

	public double getFifteenMinuteRate() {
		return metric.getFifteenMinuteRate() * rateFactor;
	}

	public String getRateUnit() {
		return rateUnit;
	}

	private String calculateRateUnit(TimeUnit unit) {
		final String s = unit.toString().toLowerCase(Locale.US);
		return s.substring(0, s.length() - 1);
	}
}