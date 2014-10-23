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


package com.torodb;

import com.torodb.torod.core.Session;
import com.torodb.torod.core.config.TorodConfig;
import com.torodb.torod.db.executor.ExecutorServiceProvider;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import javax.inject.Inject;

/**
 *
 */
public class DefaultExecutorServiceProvider implements ExecutorServiceProvider {

    private final int stripes;
    private final ExecutorService[] sessionServices;
    private final ExecutorService systemExecutor;

    @Inject
    public DefaultExecutorServiceProvider(TorodConfig config) {
        this.stripes = config.getSessionExecutorThreads();
        sessionServices = new ExecutorService[stripes];
        for (int i = 0; i < stripes; i++) {
            sessionServices[i] = Executors.newSingleThreadExecutor(new MyThreadFactory("toro-session-"+i));
        }
        this.systemExecutor = Executors.newSingleThreadExecutor(new MyThreadFactory("toro-system"));
    }

    @Override
    public void shutdown() {
        systemExecutor.shutdown();
        for (ExecutorService sessionService : sessionServices) {
            sessionService.shutdown();
        }
    }

    @Override
    public void shutdownNow() {
        systemExecutor.shutdownNow();
        for (ExecutorService sessionService : sessionServices) {
            sessionService.shutdownNow();
        }
    }
    
    @Override
    public ExecutorService consumeSystemExecutorService() {
        return systemExecutor;
    }

    @Override
    public ExecutorService consumeSessionExecutorService(Session session) {
        int index = session.hashCode() % stripes;
        if (index < 0) {
            assert index >= -stripes;
            index += stripes;
        }
        return sessionServices[index];
    }

    @Override
    public void releaseExecutorService(ExecutorService service) {
        //nothing to do
    }
    
    private static class MyThreadFactory implements ThreadFactory {

        private final String threadName;

        public MyThreadFactory(String threadName) {
            this.threadName = threadName;
        }
        
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, threadName);
        }
        
    }
}
