/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.torodb.di;

import com.google.inject.AbstractModule;
import com.torodb.torod.core.executor.ExecutorFactory;
import com.torodb.torod.db.backends.executor.DefaultExecutorFactory;
import com.torodb.torod.db.backends.executor.ExecutorServiceProvider;
import com.torodb.torod.db.backends.executor.report.DummyReportFactory;
import com.torodb.torod.db.backends.executor.report.ReportFactory;
import com.torodb.torod.db.backends.executor.servicefactory.LazyBlockingExecutorServiceProvider;
import com.torodb.torod.db.backends.executor.servicefactory.MemBlockingExecutorServiceProvider;
import javax.inject.Singleton;

/**
 *
 */
public class ExecutorModule extends AbstractModule {

    private final int maxQueueSize;
    private final long maxWaitTime;
    private final double grantedMemPercentage;

    public ExecutorModule(int maxQueueSize, long maxWaitTime, double grantedMemPercentage) {
        this.maxQueueSize = maxQueueSize;
        this.maxWaitTime = maxWaitTime;
        this.grantedMemPercentage = grantedMemPercentage;
    }
    
    @Override
    protected void configure() {
        bind(ExecutorFactory.class).to(DefaultExecutorFactory.class).in(Singleton.class);
        bind(ExecutorServiceProvider.class).toInstance(
                new MemBlockingExecutorServiceProvider(
                        grantedMemPercentage, 
                        maxWaitTime, 
                        new LazyBlockingExecutorServiceProvider(maxQueueSize)
                )
        );
        bind(ReportFactory.class)
                .toInstance(DummyReportFactory.getInstance());
    }
}
