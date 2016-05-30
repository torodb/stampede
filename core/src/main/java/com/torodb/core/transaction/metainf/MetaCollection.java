
package com.torodb.core.transaction.metainf;

import com.torodb.core.TableRef;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 */
public interface MetaCollection {

    /**
     * The name of the collection on the doc model.
     * @return
     */
    @Nonnull
    public abstract String getName();

    /**
     * The identifier of the collection on the SQL model.
     * @return
     */
    @Nonnull
    public abstract String getIdentifier();

    public abstract Stream<? extends MetaDocPart> streamContainedMetaDocParts();

    @Nullable
    public abstract MetaDocPart getMetaDocPartByIdentifier(String docPartId);

    @Nullable
    public abstract MetaDocPart getMetaDocPartByTableRef(TableRef tableRef);
    
}
