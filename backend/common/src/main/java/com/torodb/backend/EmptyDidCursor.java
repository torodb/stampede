
package com.torodb.backend;

import com.torodb.core.backend.DidCursor;
import java.util.Collection;
import java.util.Collections;
import java.util.NoSuchElementException;

/**
 *
 */
public class EmptyDidCursor implements DidCursor {

    public static final EmptyDidCursor INSTANCE = new EmptyDidCursor();

    private EmptyDidCursor() {}

    @Override
    public Integer next() {
        throw new NoSuchElementException();
    }

    @Override
    public Collection<Integer> getRemaining() {
        return Collections.emptyList();
    }

    @Override
    public void close() {
    }

    @Override
    public boolean hasNext() {
        return false;
    }

}
