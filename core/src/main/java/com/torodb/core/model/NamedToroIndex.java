package com.torodb.core.model;

import javax.annotation.Nonnull;

/**
 *
 */
public interface NamedToroIndex extends ToroIndex {

    @Nonnull
    public String getName();
    
}
