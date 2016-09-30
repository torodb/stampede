
package com.torodb.core.transaction.metainf;

import java.util.Iterator;
import java.util.List;
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
    
    public abstract Iterator<? extends MetaIndexField> iteratorFields();
    
    public abstract Iterator<? extends ImmutableMetaIndexField> iteratorMetaIndexFieldByTableRef(TableRef tableRef);
    
    public abstract Stream<TableRef> streamTableRefs();

    @Nullable
    public abstract MetaIndexField getMetaIndexFieldByTableRefAndName(TableRef tableRef, String name);
    
    @Nullable
    public abstract MetaIndexField getMetaIndexFieldByTableRefAndPosition(TableRef tableRef, int position);

    @Nullable
    public abstract MetaIndexField getMetaIndexFieldByPosition(int position);
    
    public abstract Iterator<List<String>> iteratorMetaDocPartIndexesIdentifiers(MetaDocPart docPart);
    
    public abstract boolean isCompatible(MetaDocPart docPart);
    
    public abstract boolean isCompatible(MetaDocPart docPart, MetaDocPartIndex docPartIndex);
    
    public abstract boolean isMatch(MetaDocPart docPart, List<String> identifiers, MetaDocPartIndex docPartIndex);

    public default String defautToString() {
        return "index{" + "name:" + getName() + ", unique:" + isUnique() + '}';
    }
    
}
