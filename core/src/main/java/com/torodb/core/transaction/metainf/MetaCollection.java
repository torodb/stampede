
package com.torodb.core.transaction.metainf;

import com.torodb.core.TableRef;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * @param <MDP>
 */
public interface MetaCollection<MDP extends MetaDocPart> {

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

    public abstract Stream<MDP> streamContainedMetaDocParts();

    @Nullable
    public abstract MDP getMetaDocPartByIdentifier(String tableDbName);

    @Nullable
    public abstract MDP getMetaDocPartByTableRef(TableRef tableRef);
    
}
