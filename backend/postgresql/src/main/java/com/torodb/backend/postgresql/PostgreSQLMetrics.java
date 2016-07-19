package com.torodb.backend.postgresql;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.torodb.core.metrics.MetricNameFactory;
import com.torodb.core.metrics.ToroMetricRegistry;

@Singleton
public class PostgreSQLMetrics {

	private static final MetricNameFactory factory = new MetricNameFactory("PostgreSQLWrite");
	public final Timer insertDocPartDataTimer;
	public final Meter insertRows;
	public final Meter insertFields;
	
	@Inject
	public PostgreSQLMetrics(ToroMetricRegistry registry){
		insertDocPartDataTimer = registry.timer(factory.createMetricName("insertDocPartDataTimer"));
		insertRows = registry.meter(factory.createMetricName("insertRows"));
		insertFields = registry.meter(factory.createMetricName("insertFields"));
	}
	
}
