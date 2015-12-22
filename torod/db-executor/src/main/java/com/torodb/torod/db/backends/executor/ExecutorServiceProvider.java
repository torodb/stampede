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


package com.torodb.torod.db.backends.executor;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.torodb.torod.core.Session;
import java.util.concurrent.ExecutorService;

/**
 *
 */
public interface ExecutorServiceProvider {

    ListeningExecutorService consumeSystemExecutorService();
    
    ListeningExecutorService consumeSessionExecutorService(Session session);
    
    void releaseExecutorService(ExecutorService service);
    
    public void shutdown();
    
    public void shutdownNow();
    
}
