
package com.torodb.torod.mongodb.repl;

import com.eightkdata.mongowp.mongoserver.api.safe.oplog.OplogOperation;
import com.eightkdata.mongowp.mongoserver.pojos.OpTime;
import com.google.common.util.concurrent.AbstractIdleService;
import com.torodb.torod.mongodb.repl.OplogManager.WriteTransaction;
import com.torodb.torod.mongodb.repl.exceptions.EmptyOplogException;
import com.torodb.torod.mongodb.repl.exceptions.InvalidOplogOperation;
import java.util.concurrent.Executor;

/**
 *
 */
class RecoveryService extends AbstractIdleService {

    private final Callback callback;
    private final OplogManager oplogManager;
    private final OplogReader reader;
    private final Executor executor;

    RecoveryService(Callback callback, OplogManager oplogManager, OplogReader reader, Executor executor) {
        this.callback = callback;
        this.oplogManager = oplogManager;
        this.reader = reader;
        this.executor = executor;
    }

    @Override
    protected Executor executor() {
        return executor;
    }

    @Override
    protected void startUp() {
        try {
            reader.connect(OpTime.EPOCH);
            OplogOperation firstOp = reader.getFirstOp();
            WriteTransaction trans = oplogManager.createWriteTransaction();
            try {
                trans.addOperation(firstOp);
            } finally {
                trans.close();
            }

            callback.recoveryFinished();
        } catch (EmptyOplogException ex) {
            callback.recoveryFailed(ex);
        } catch (InvalidOplogOperation ex) {
            callback.recoveryFailed(ex);
        } catch (Throwable ex) {
            callback.recoveryFailed(ex);
        }
    }

    @Override
    protected void shutDown() {
        reader.close();
    }

    static interface Callback {
        void recoveryFinished();

        void recoveryFailed(Throwable ex);
    }

}
