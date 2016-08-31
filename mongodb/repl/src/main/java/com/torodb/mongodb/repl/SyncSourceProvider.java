
package com.torodb.mongodb.repl;

import javax.annotation.Nullable;

import com.eightkdata.mongowp.OpTime;
import com.google.common.net.HostAndPort;
import com.torodb.mongodb.repl.exceptions.NoSyncSourceFoundException;

/**
 *
 */
public interface SyncSourceProvider {

    public HostAndPort calculateSyncSource(@Nullable HostAndPort oldSyncSource)
            throws NoSyncSourceFoundException;

    /**
     * Returns the host and port of the server that must be used to read from.
     * <p>
     * The host and port is decided by the using the given optime and the old reader.
     * <p>
     * @param lastFetchedOpTime the optime of the last fetched operation
     * @return
     * @throws NoSyncSourceFoundException iff there is no sync source we can
     *                                    reply from using the given optime
     */
    public HostAndPort getSyncSource(OpTime lastFetchedOpTime) throws NoSyncSourceFoundException;

    public HostAndPort getLastUsedSyncSource();
}
