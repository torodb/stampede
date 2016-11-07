
package com.torodb.mongodb.repl;


import com.eightkdata.mongowp.OpTime;
import com.google.common.net.HostAndPort;
import com.torodb.mongodb.repl.exceptions.NoSyncSourceFoundException;
import java.util.Optional;

/**
 *
 */
public interface SyncSourceProvider {

    public HostAndPort newSyncSource() throws NoSyncSourceFoundException;

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
    public default HostAndPort newSyncSource(OpTime lastFetchedOpTime) throws NoSyncSourceFoundException{
        return newSyncSource();
    }

    public Optional<HostAndPort> getLastUsedSyncSource();

    public boolean shouldChangeSyncSource();

}
