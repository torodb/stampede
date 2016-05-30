
package com.torodb.core.transaction.metainf;

import com.torodb.core.annotations.DoNotChange;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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

    @DoNotChange
    public abstract Iterable<? extends MutableMetaDatabase> getModifiedDatabases();

    @Nonnull
    public abstract ImmutableMetaSnapshot immutableCopy();
}
