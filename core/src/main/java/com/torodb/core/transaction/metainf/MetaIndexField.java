
package com.torodb.core.transaction.metainf;

import javax.annotation.Nonnull;

import com.torodb.core.TableRef;

/**
 *
 */
public interface MetaIndexField {

    @Nonnull
    public abstract int getPosition();
    
    @Nonnull
    public abstract TableRef getTableRef();

    /**
     * The name of the field to index on the document model.
     * @return
     */
    @Nonnull
    public abstract String getName();

    @Nonnull
    public abstract FieldIndexOrdering getOrdering();
    
    public abstract boolean isCompatible(MetaDocPart docPart);
    
    public abstract boolean isCompatible(MetaDocPart docPart, MetaDocPartIndexColumn indexColumn);
    
    public abstract boolean isMatch(MetaDocPart docPart, String identifier, MetaDocPartIndexColumn indexColumn);

    public default String defautToString() {
        return "indexField{" + "position:" + getPosition() + ", tableRef:" + getTableRef() + ", name:" + getName() + ", ordering:" + getOrdering() + '}';
    }
}
