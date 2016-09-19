
package com.torodb.core.transaction.metainf;

import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 */
public interface MetaDocPartIndex {

    /**
     * The identifier MetaDocPart on the SQL model.
     * @return
     */
    @Nonnull
    public abstract String getIdentifier();

    public abstract boolean isUnique();

    public abstract int size();
    
    public abstract Stream<? extends MetaFieldIndex> streamFields();

    @Nullable
    public abstract MetaFieldIndex getMetaFieldIndexByPosition(int position);

    @Nullable
    public abstract MetaFieldIndex getMetaFieldIndexByNameAndType(String fieldName, FieldType type);

    public default String defautToString() {
        return "docPartIndex{" + "id:" + getIdentifier() + ", unique:" + isUnique() + '}';
    }
    
}
