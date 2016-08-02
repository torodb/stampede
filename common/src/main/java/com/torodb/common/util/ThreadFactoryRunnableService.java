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
 * along with common. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.common.util;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;

/**
 *
 */
public abstract class ThreadFactoryRunnableService extends AbstractExecutionThreadService {

    private final ThreadFactory threadFactory;

    protected ThreadFactoryRunnableService(ThreadFactory threadFactory) {
        this.threadFactory = threadFactory;
    }

    @Override
    protected Executor executor() {
        return (Runnable command) -> {
            Thread thread = threadFactory.newThread(command);
            thread.start();
        };
    }

    /**
     * The method that change the name of the given thread.
     *
     * By default the used name is the service name plus the state, but subclasses can change it
     * (or even do not change the default thread name provided by the thread factory).
     * @param thread the thred whose name can be changed.
     */
    protected void changeThreadName(Thread thread) {
        thread.setName(serviceName() + " " + state());
    }

}
