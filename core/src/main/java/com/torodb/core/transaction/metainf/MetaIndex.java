
package com.torodb.core.transaction.metainf;

import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.torodb.core.TableRef;

/**
 */
public interface MetaIndex {

    /**
     * The name of the index on the doc model.
     * @return
     */
    @Nonnull
    public abstract String getName();

    public abstract boolean isUnique();

    public abstract int size();
    
    public abstract Stream<? extends MetaIndexField> streamFields();
    
    public abstract Stream<? extends ImmutableMetaIndexField> streamMetaIndexFieldByTableRef(TableRef tableRef);

    @Nullable
    public abstract MetaIndexField getMetaIndexFieldByTableRefAndName(TableRef tableRef, String name);

    @Nullable
    public abstract MetaIndexField getMetaIndexFieldByPosition(int position);

    public default String defautToString() {
        return "index{" + "name:" + getName() + ", unique:" + isUnique() + '}';
    }
    
}
