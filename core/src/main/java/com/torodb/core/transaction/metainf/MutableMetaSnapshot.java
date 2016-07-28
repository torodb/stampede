
package com.torodb.core.transaction.metainf;

import com.torodb.core.annotations.DoNotChange;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.jooq.lambda.tuple.Tuple2;

/**
 *
 * @param <MMD>
 */
public interface MutableMetaSnapshot extends MetaSnapshot {

    @Override
    @Nullable
    public MutableMetaDatabase getMetaDatabaseByIdentifier(String dbIdentifier);

    @Override
    @Nullable
    public MutableMetaDatabase getMetaDatabaseByName(String dbName);

    @Override
    public Stream<? extends MutableMetaDatabase> streamMetaDatabases();

    @Nonnull
    public abstract MutableMetaDatabase addMetaDatabase(String dbName, String dbId) throws IllegalArgumentException;

    /**
     * Removes a meta database selected by its name.
     * @param dbName
     * @return true iff the meta database was removed
     */
    public abstract boolean removeMetaDatabaseByName(String dbName);

    /**
     * REmoves a meta database selected by its identifier
     * @param dbId
     * @return true iff the meta database was removed
     */
    public abstract boolean removeMetaDatabaseByIdentifier(String dbId);

    @DoNotChange
    public abstract Iterable<Tuple2<MutableMetaDatabase, MetaElementState>> getModifiedDatabases();

    public abstract boolean hasChanged();

    @Nonnull
    public abstract ImmutableMetaSnapshot immutableCopy();

    public default boolean containsMetaDatabaseByName(String dbName) {
        return getMetaDatabaseByName(dbName) != null;
    }

    public default boolean containsMetaDatabaseByIdentifier(String dbId) {
        return getMetaDatabaseByIdentifier(dbId) != null;
    }
}
