
package com.torodb.core.transaction.metainf;

import java.util.List;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.jooq.lambda.tuple.Tuple2;

import com.torodb.core.TableRef;

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


    public Stream<? extends MetaIndex> streamContainedMetaIndexes();

    @Nullable
    public MetaIndex getMetaIndexByName(String indexName);

    public List<Tuple2<MetaIndex, List<String>>> getMissingIndexesForNewField(
            MutableMetaDocPart docPart, MetaField newField);
    
    public default String defautToString() {
        return "col{" + "name:" + getName() + ", id:" + getIdentifier() + '}';
    }
    
}
