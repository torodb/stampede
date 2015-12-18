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
package com.torodb.torod.db.backends.executor.servicefactory;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.torodb.torod.core.Session;
import com.torodb.torod.db.backends.executor.ExecutorServiceProvider;

import java.util.concurrent.ExecutorService;

/**
 *
 */
public abstract class AbstractExecutorServiceProvider implements ExecutorServiceProvider {
    
    private ListeningExecutorService systemExecutorService;

    protected abstract ExecutorService createExecutorService(String name, int priority);

    @Override
    public ListeningExecutorService consumeSystemExecutorService() {
        if (systemExecutorService == null) {
            systemExecutorService = 
                    MoreExecutors.listeningDecorator(
                            createExecutorService("torod-system", Thread.NORM_PRIORITY + 1)
                    );
        }
        return systemExecutorService;
    }

    @Override
    public ListeningExecutorService consumeSessionExecutorService(Session session) {
        return MoreExecutors.listeningDecorator(
                createExecutorService("torod-session", Thread.NORM_PRIORITY)
        );
    }

    @Override
    public void releaseExecutorService(ExecutorService service) {
        service.shutdown();
    }

    @Override
    public void shutdown() {
        if (systemExecutorService != null) {
            systemExecutorService.shutdown();
        }
    }

    @Override
    public void shutdownNow() {
        if (systemExecutorService != null) {
            systemExecutorService.shutdownNow();
        }
    }
}
