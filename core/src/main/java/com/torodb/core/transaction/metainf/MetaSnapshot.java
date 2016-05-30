
package com.torodb.core.transaction.metainf;

import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 *
 */
public interface MetaSnapshot<MD extends MetaDatabase> {

    public Stream<MD> streamMetaDatabases();

    @Nullable
    public MD getMetaDatabaseByName(String dbName);

    @Nullable
    public MD getMetaDatabaseByIdentifier(String dbIdentifier);
}
