
package com.torodb.core.transaction.metainf;

import com.torodb.core.annotations.DoNotChange;
import java.util.stream.Stream;

/**
 *
 * @param <MMD>
 */
public interface MutableMetaSnapshot extends MetaSnapshot {

    @Override
    public MutableMetaDatabase getMetaDatabaseByIdentifier(String dbIdentifier);

    @Override
    public MutableMetaDatabase getMetaDatabaseByName(String dbName);

    @Override
    public Stream<? extends MutableMetaDatabase> streamMetaDatabases();

    public abstract MutableMetaDatabase addMetaDatabase(String dbName, String dbId);

    @DoNotChange
    public abstract Iterable<? extends MutableMetaDatabase> getModifiedDatabases();

    public abstract ImmutableMetaSnapshot immutableCopy();
}
