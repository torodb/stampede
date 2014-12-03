package com.torodb.torod.core.pojos;

import javax.annotation.Nonnull;

/**
 *
 */
public interface NamedToroIndex extends ToroIndex {

    @Nonnull
    public String getName();
    
}
