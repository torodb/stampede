package com.torodb.mongodb.repl;

import java.util.Locale;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.codahale.metrics.Counter;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.MemberState;
import com.torodb.core.metrics.MetricNameFactory;
import com.torodb.core.metrics.SettableGauge;
import com.torodb.core.metrics.ToroMetricRegistry;

@Singleton
public class ReplMetrics {

	private static final MetricNameFactory factory = new MetricNameFactory("Repl");
	public final SettableGauge<String> memberState;
    public final Counter[] memberStateCounters;
    public final SettableGauge<String> oplogStatOpTime;
	
	@Inject
	public ReplMetrics(ToroMetricRegistry registry){
        memberState = registry.gauge(factory.createMetricName("currentMemberState"));
	    memberStateCounters = new Counter[MemberState.values().length];
	    for (MemberState memberState : MemberState.values()) {
	        memberStateCounters[memberState.ordinal()] = 
	                registry.counter(factory.createMetricName(
	                        memberState.name().substring(3).toLowerCase(Locale.US) + "Count"));
	    }
        oplogStatOpTime = registry.gauge(factory.createMetricName("lastOplogStatOpTime"));
	}
}
