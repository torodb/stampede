package com.torodb.mongodb.repl;

import java.util.Locale;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.codahale.metrics.Counter;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.MemberState;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.torodb.core.metrics.MetricNameFactory;
import com.torodb.core.metrics.SettableGauge;
import com.torodb.core.metrics.ToroMetricRegistry;

@Singleton
public class ReplMetrics {

	private static final MetricNameFactory factory = new MetricNameFactory("Repl");
	private final SettableGauge<String> memberState;
    private final ImmutableMap<MemberState, Counter> memberStateCounters;
    private final SettableGauge<String> lastOpTimeFetched;
    private final SettableGauge<String> lastOpTimeApplied;
	
	@Inject
	public ReplMetrics(ToroMetricRegistry registry){
        memberState = registry.gauge(factory.createMetricName("currentMemberState"));
        ImmutableMap.Builder<MemberState, Counter> memberStateCountersBuilder =
                ImmutableMap.builder();
	    for (MemberState memberState : MemberState.values()) {
	        memberStateCountersBuilder.put(memberState, 
	                registry.counter(factory.createMetricName(
	                        memberState.name().substring(3).toLowerCase(Locale.US) + "Count")));
	    }
        memberStateCounters = Maps.immutableEnumMap(memberStateCountersBuilder.build());
        lastOpTimeFetched = registry.gauge(factory.createMetricName("lastOpTimeFetched"));
        lastOpTimeApplied = registry.gauge(factory.createMetricName("lastOpTimeApplied"));
	}

    public SettableGauge<String> getMemberState() {
        return memberState;
    }

    public ImmutableMap<MemberState, Counter> getMemberStateCounters() {
        return memberStateCounters;
    }

    public SettableGauge<String> getLastOpTimeFetched() {
        return lastOpTimeFetched;
    }

    public SettableGauge<String> getLastOpTimeApplied() {
        return lastOpTimeApplied;
    }
}
