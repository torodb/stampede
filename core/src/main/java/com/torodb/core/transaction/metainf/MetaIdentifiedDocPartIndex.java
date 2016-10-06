
package com.torodb.core.transaction.metainf;

import javax.annotation.Nonnull;

/**
 */
public interface MetaIdentifiedDocPartIndex extends MetaDocPartIndex {

    /**
     * The identifier MetaDocPart on the SQL model.
     * @return
     */
    @Nonnull
    public abstract String getIdentifier();

    public default String defautToString() {
        return "docPartIndex{" + "id:" + getIdentifier() + ", unique:" + isUnique() + '}';
    }
    
}
