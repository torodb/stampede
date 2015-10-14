
package com.torodb.util.mgl;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 *
 * @param <HMGL> 
 * @param <ChildId> The class used to identify childs of this class
 */
@ThreadSafe
public interface HierarchicalMGLock<HMGL extends HierarchicalMGLock, ChildId> extends MultipleGranularityLock {

    /**
     * 
     * @return the parent of this node or null if this is the root
     */
    @Nullable
    public HMGL getParent() throws UnsupportedOperationException;

    /**
     *
     * @param id
     * @return the created child
     * @throws IllegalArgumentException if there is already a child with the given id
     */
    @Nonnull
    public HMGL createChild(ChildId id) throws IllegalArgumentException;

    /**
     *
     * @param id
     * @return if there was a child with the given id
     */
    public boolean removeChild(ChildId id);

    @Nonnull
    public HMGL getOrCreateChild(ChildId id);

    @Nullable
    public HMGL getChild(ChildId id);

}
