
package com.torodb.util.mgl;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * This is a trivial implementation of {@link HierarchicalMGLock}
 * that always lock the root of the hierarchy node.
 *
 * This lock is very unefficient. You should always use another implementation
 */
@ThreadSafe
public class RootLockedMultipleGranularityLock<Id> implements HierarchicalMGLock<RootLockedMultipleGranularityLock, Id>{

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    @Override
    public RootLockedMultipleGranularityLock getParent() {
        return this;
    }

    @Override
    public RootLockedMultipleGranularityLock createChild(Id id) {
        return this;
    }

    @Override
    public RootLockedMultipleGranularityLock getOrCreateChild(Id id) {
        return this;
    }

    @Override
    public boolean removeChild(Id id) {
        return true;
    }

    @Override
    public RootLockedMultipleGranularityLock getChild(Id id) {
        return this;
    }

    @Nonnull
    private Lock getLock(Mode mode) {
        switch (mode) {
            case IS:
            case S:
                return rwLock.readLock();
            case IX:
            case X:
                return rwLock.writeLock();
            default:
                throw new IllegalArgumentException(mode + " mode is not supported");
        }
    }

    @Override
    public Releaseable adquire(Mode mode) {
        Lock lock = getLock(mode);
        lock.lock();
        return new MyReleaseable(lock);
    }

    @Override
    public Releaseable adquire(Mode mode, long timeout) throws
            InterruptedException {
        Lock lock = getLock(mode);
        if (lock.tryLock(timeout, TimeUnit.MILLISECONDS)) {
            return new MyReleaseable(lock);
        }
        return null;
    }

    @NotThreadSafe
    private static class MyReleaseable implements Releaseable {

        private boolean closed;
        private final Lock lock;

        public MyReleaseable(Lock lock) {
            this.lock = lock;
            closed = false;
        }

        @Override
        public void close() {
            if (!closed) {
                lock.unlock();
                closed = true;
            }
        }

    }

}
