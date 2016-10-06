
package com.torodb.core.transaction.metainf;

import java.util.Iterator;

import javax.annotation.Nullable;

/**
 */
public interface MetaDocPartIndex {

    public abstract boolean isUnique();

    public abstract int size();
    
    public abstract Iterator<? extends MetaDocPartIndexColumn> iteratorColumns();

    @Nullable
    public abstract MetaDocPartIndexColumn getMetaDocPartIndexColumnByPosition(int position);

    @Nullable
    public abstract MetaDocPartIndexColumn getMetaDocPartIndexColumnByIdentifier(String columnName);
    
    public abstract boolean hasSameColumns(MetaDocPartIndex docPartIndex);

    public default String defautToString() {
        return "docPartIndex{" + "unique:" + isUnique() + '}';
    }
    
}
