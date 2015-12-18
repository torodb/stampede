package com.torodb.util.mgl;

import java.io.Closeable;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Objects of this class are locks which belong to a tree lock hierarchy, aka
 * <a href="https://en.wikipedia.org/wiki/Multiple_granularity_locking">Multiple granularity locking</a>.
 *
 *
 * @see <a href="https://en.wikipedia.org/wiki/Multiple_granularity_locking">Multiple granularity locking wikipedia page</a>.
 */
@ThreadSafe
public interface MultipleGranularityLock {

    /**
     * The current thread stops until it can adquire this lock in the given mode.
     *
     * All parent nodes are modified as specified by the
     * <a href="https://en.wikipedia.org/wiki/Multiple_granularity_locking">MGL algorithm</a>
     * so you do not need (but you can if you want) adquire the parent nodes.
     * @param mode
     * @return
     */
    @Nonnull
    public Releaseable adquire(Mode mode);

    @Nullable
    public Releaseable adquire(Mode mode, long timeout) 
            throws InterruptedException;

    public static interface Releaseable extends Closeable {

        @Override
        public void close();

    }
}
