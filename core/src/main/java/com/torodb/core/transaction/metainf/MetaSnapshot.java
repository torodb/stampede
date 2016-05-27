
package com.torodb.core.transaction.metainf;

import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 *
 */
public interface MetaSnapshot<MD extends MetaDatabase> {

    public Stream<MD> streamDatabases();

    @Nullable
    public MD getDatabaseByName(String dbName);

    @Nullable
    public MD getDatabaseByIdentifier(String dbIdentifier);
}
