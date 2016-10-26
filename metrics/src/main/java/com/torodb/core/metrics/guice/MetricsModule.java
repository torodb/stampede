package com.torodb.core.metrics.guice;

import com.google.inject.PrivateModule;
import com.google.inject.Singleton;
import com.torodb.core.metrics.DisabledMetricRegistry;
import com.torodb.core.metrics.MetricsConfig;
import com.torodb.core.metrics.ToroMetricRegistry;

public class MetricsModule extends PrivateModule {
	
	private MetricsConfig config;
	
	public MetricsModule(MetricsConfig config){
		this.config = config;
	}

	@Override
	protected void configure() {

		if (!config.getMetricsEnabled()){
            bind(DisabledMetricRegistry.class)
                    .in(Singleton.class);
            bind(ToroMetricRegistry.class)
                .to(DisabledMetricRegistry.class);
		} else {
            bind(ToroMetricRegistry.class)
                .in(Singleton.class);
        }
        expose(ToroMetricRegistry.class);
	}

}
