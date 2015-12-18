
package com.torodb.torod.mongodb.repl.exceptions;

import com.eightkdata.mongowp.mongoserver.pojos.OpTime;

/**
 *
 */
public class NoSyncSourceFoundException extends Exception {
    private static final long serialVersionUID = 1L;
    private final OpTime lastFetchedOpTime;

    public NoSyncSourceFoundException() {
        this.lastFetchedOpTime = null;
    }

    public NoSyncSourceFoundException(OpTime lastFetchedOpTime) {
        this.lastFetchedOpTime = lastFetchedOpTime;
    }

    public OpTime getLastFetchedOpTime() {
        return lastFetchedOpTime;
    }

}
