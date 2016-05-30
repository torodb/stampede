
package com.torodb.core.transaction.metainf;

import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 *
 */
public interface MetaSnapshot {

    public Stream<? extends MetaDatabase> streamMetaDatabases();

    @Nullable
    public MetaDatabase getMetaDatabaseByName(String dbName);

    @Nullable
    public MetaDatabase getMetaDatabaseByIdentifier(String dbIdentifier);
}
