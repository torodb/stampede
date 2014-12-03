
package com.torodb.torod.core.pojos;

import javax.annotation.Nonnull;

/**
 *
 */
public interface ToroIndex {

    @Nonnull
    public IndexedAttributes getAttributes();

    @Nonnull
    public String getDatabase();

    @Nonnull
    public String getCollection();

    public boolean isUnique();
    
    public UnnamedToroIndex asUnnamed();

}
