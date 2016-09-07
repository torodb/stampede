/*
 * This file is part of ToroDB.
 *
 * ToroDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ToroDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with repl. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.mongodb.repl.oplogreplier;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.torodb.core.metrics.MetricNameFactory;
import com.torodb.core.metrics.ToroMetricRegistry;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 *
 */
@Singleton
public class OplogApplierMetrics {

    private final Timer maxDelay;
    private final Meter applied;
    private final Histogram batchSize;
    private final Histogram applicationCost;

    @Inject
    public OplogApplierMetrics(ToroMetricRegistry registry) {
        MetricNameFactory factory = new MetricNameFactory("OplogApplier");

        maxDelay = registry.timer(factory.createMetricName("maxDelay"));

        applied = registry.meter(factory.createMetricName("applied"));
        registry.gauge(factory.createMetricName("appliedUnit")).setValue("ops");

        batchSize = registry.histogram(factory.createMetricName("batchSize"));
        registry.gauge(factory.createMetricName("batchSizeUnit")).setValue("ops/batch");

        applicationCost = registry.histogram(factory.createMetricName("applicationCost"));
        registry.gauge(factory.createMetricName("applicationCostUnit")).setValue("microseconds/op");
    }
    
    public Timer getMaxDelay() {
        return maxDelay;
    }

    public Meter getApplied() {
        return applied;
    }

    public Histogram getBatchSize() {
        return batchSize;
    }

    public Histogram getApplicationCost() {
        return applicationCost;
    }
}
