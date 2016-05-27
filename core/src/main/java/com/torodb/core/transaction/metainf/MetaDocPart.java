
package com.torodb.core.transaction.metainf;

import com.torodb.core.TableRef;
import com.torodb.core.annotations.DoNotChange;
import com.torodb.kvdocument.types.KVType;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * @param <MF>
 */
public interface MetaDocPart<MF extends MetaField> {

    @Nonnull
    public abstract TableRef getTableRef();

    /**
     * The identifier MetaDocPart on the SQL model.
     * @return
     */
    @Nonnull
    public abstract String getIdentifier();

    @Nonnull
    @DoNotChange
    public abstract Stream<MF> streamFields();

    /**
     *
     * @param columnDbName
     * @return the contained column whose {@link MetaField#getDbName() db name} is the given or null
     *         if there is no one that match that condition
     */
    @Nullable
    public abstract MF getMetaFieldByIdentifier(String columnDbName);

    /**
     *
     * @param columnDocName
     * @return the contained columns whose {@link MetaField#getDocName() db name} is the given or an
     *         empty list if there is no one that match that condition
     */
    public abstract Stream<MF> streamMetaFieldByName(String columnDocName);

    /**
     * 
     * @param columnDocName
     * @param type
     * @return the contained column whose {@link MetaField#getDocName() db name} and whose
     *         {@link MetaField#getType() type} is the given or null if there is no one that match
     *         that condition
     */
    @Nullable
    public abstract MF getMetaFieldByNameAndType(String columnDocName, KVType type);
}
