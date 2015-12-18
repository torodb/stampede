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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 */
public class Monitor {
    
    private final int conditionStrips;
    private final Lock[] locks;
    private final Condition[] conditions;
    private final AtomicLong tickCounter;

    public Monitor(long initialTick, int conditionStrips) {
        this.tickCounter = new AtomicLong(initialTick);
        this.conditionStrips = conditionStrips;
        this.locks = new Lock[conditionStrips];
        this.conditions = new Condition[conditionStrips];
        
        for (int i = 0; i < conditionStrips; i++) {
            locks[i] = new ReentrantLock();
            conditions[i] = locks[i].newCondition();
        }
    }
    
    private Lock getLock(long tick) {
        int intTick = (int) (tick & 0xFFFFFFFF);
        return locks[intTick % conditionStrips];
    }
    
    private Condition getCondition(long tick) {
        int intTick = (int) (tick & 0xFFFFFFFF);
        return conditions[intTick % conditionStrips];
    }
    
    public void awaitFor(long tick) throws InterruptedException {
        if (tickCounter.get() >= tick) {
            return ;
        }
        
        Lock lock = getLock(tick);
        lock.lock();
        try {
            while (tickCounter.get() < tick) {
                getCondition(tick).await();
            }
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * Increments the tick.
     */
    public void tick() {
        long actualValue = tickCounter.incrementAndGet();
        
        Lock lock = getLock(actualValue);
        lock.lock();
        try {
            getCondition(actualValue).signalAll();
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * @return the last tick
     */
    public long getTick() {
        return tickCounter.get();
    }
    
}
