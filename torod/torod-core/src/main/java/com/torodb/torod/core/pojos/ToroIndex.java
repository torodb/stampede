
package com.torodb.torod.core.pojos;

import java.io.Serializable;
import javax.annotation.Nonnull;

/**
 *
 */
public interface ToroIndex extends Serializable {

    @Nonnull
    public IndexedAttributes getAttributes();

    @Nonnull
    public String getDatabase();

    @Nonnull
    public String getCollection();

    public boolean isUnique();
    
    public UnnamedToroIndex asUnnamed();

}
