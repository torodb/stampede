
package com.torodb.core.model;

import java.io.Serializable;

import javax.annotation.Nonnull;

import com.torodb.core.language.AttributeReference;
import com.torodb.core.model.IndexedAttributes.IndexType;

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
