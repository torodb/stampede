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

import com.torodb.torod.core.dbWrapper.DbWrapper;
import com.torodb.torod.core.executor.ToroTaskExecutionException;
import com.torodb.torod.db.backends.executor.report.DummyReportFactory;
import java.util.concurrent.ExecutorService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.stubbing.answers.DoesNothing;
import org.mockito.internal.stubbing.answers.ThrowsException;
import org.mockito.stubbing.Answer;

/**
 *
 */
public class DefaultSystemExecutorTest_tick {
    
    DefaultSystemExecutor executor;
    
    @Before
    public void setUp() {
        Answer exceptionAnswer = new ThrowsException(new AssertionError());
        ExceptionHandler exceptionHandler = Mockito.mock(ExceptionHandler.class, exceptionAnswer);
        DbWrapper wrapper = Mockito.mock(DbWrapper.class, exceptionAnswer);
        ExecutorServiceProvider executorServiceProvider = Mockito.mock(ExecutorServiceProvider.class, exceptionAnswer);
        ExecutorService executorService = Mockito.mock(ExecutorService.class, new DoesNothing());
        Mockito.doReturn(executorService).when(executorServiceProvider).consumeSystemExecutorService();
        Monitor monitor = Mockito.mock(Monitor.class, exceptionAnswer);
        long initialTick = 0;
        
        
        executor = new DefaultSystemExecutor(
                exceptionHandler, 
                wrapper, 
                executorServiceProvider, 
                monitor, 
                initialTick,
                DummyReportFactory.getInstance()
        );
    }
    
    @Test
    public void testGetTick_createCollection() throws ToroTaskExecutionException {
        
        
        long tick1 = executor.getTick();
        
        executor.createCollection(null, null, null);
        long tick2 = executor.getTick();
        assert tick2 == tick1 + 1 : tick2 + " != " + (tick1 + 1);
    }
        
    @Test
    public void testGetTick_createSubDocTable() throws ToroTaskExecutionException {
        long tick1 = executor.getTick();
        
        executor.createSubDocTable(null, null, null);
        long tick2 = executor.getTick();
        assert tick2 == tick1 + 1 : tick2 + " != " + (tick1 + 1);
    }
    
    @Test
    public void testGetTick_findCollections() throws ToroTaskExecutionException {
        long tick1 = executor.getTick();
        
        executor.findCollections();
        long tick2 = executor.getTick();
        assert tick2 == tick1 + 1 : tick2 + " != " + (tick1 + 1);
    }
    
    @Test
    public void testGetTick_reserveDocIds() throws ToroTaskExecutionException {
        long tick1 = executor.getTick();
        
        executor.reserveDocIds(null, 0, null);
        long tick2 = executor.getTick();
        assert tick2 == tick1 + 1 : tick2 + " != " + (tick1 + 1);
    }
    
}
