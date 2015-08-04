
package com.torodb.util.mgl;

import javax.annotation.concurrent.ThreadSafe;

/**
 *
 * @param <HMGL> 
 * @param <Id> The class used to identify childs of this class
 */
@ThreadSafe
public interface HierarchicalMultipleGranularityLock<HMGL extends HierarchicalMultipleGranularityLock, Id> extends MultipleGranularityLock {

    public HMGL getParent();

    public HMGL createChild(Id id);

    public HMGL getOrCreateChild(Id id);

    public HMGL getChild(Id id);

}
