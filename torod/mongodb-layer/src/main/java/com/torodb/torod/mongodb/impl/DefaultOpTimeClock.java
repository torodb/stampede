
package com.torodb.torod.mongodb.impl;

import com.eightkdata.mongowp.OpTime;
import com.torodb.torod.mongodb.OptimeClock;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.ThreadSafe;

/**
 *
 */
@ThreadSafe
public class DefaultOpTimeClock implements OptimeClock {

    static final long INT_MASK = 0xffffffffL;

    private final AtomicInteger seqProvider = new AtomicInteger(0);

    @Override
    public OpTime tick() {
        int secs = (int) (System.currentTimeMillis() / 1000 & INT_MASK);
        int term = seqProvider.incrementAndGet();

        return new OpTime(secs, term);
    }

}
