
package com.torodb.torod.mongodb.repl;

import com.google.common.net.HostAndPort;
import javax.annotation.Nullable;

/**
 *
 */
public interface SyncSourceProvider {

    public HostAndPort calculateSyncSource(@Nullable HostAndPort oldSyncSource);

    public HostAndPort getLastUsedSyncSource();

}
