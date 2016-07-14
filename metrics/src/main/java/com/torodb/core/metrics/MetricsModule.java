package com.torodb.core.metrics;

import com.google.inject.AbstractModule;

public class MetricsModule extends AbstractModule {
	
	private MetricsConfig config;
	
	public MetricsModule(MetricsConfig config){
		this.config = config;
	}

	@Override
	protected void configure() {
		if (!config.getMetricsEnabled()){
			bind(ToroMetricRegistry.class).to(DisabledMetricRegistry.class);
		}
	}

}
