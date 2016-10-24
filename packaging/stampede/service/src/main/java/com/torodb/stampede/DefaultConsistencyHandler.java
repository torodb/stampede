
package com.torodb.stampede;

import com.torodb.core.backend.*;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.retrier.Retrier;
import com.torodb.core.retrier.RetrierAbortException;
import com.torodb.core.retrier.RetrierGiveUpException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.kvdocument.types.BooleanType;
import com.torodb.kvdocument.values.KVBoolean;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.mongodb.repl.ConsistencyHandler;
import java.util.Optional;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 *
 */
@NotThreadSafe
public class DefaultConsistencyHandler implements ConsistencyHandler {

    private static final Logger LOGGER
            = LogManager.getLogger(DefaultConsistencyHandler.class);
    private boolean consistent;
    private static final MetaInfoKey CONSISTENCY_KEY = () -> "repl.consistent";
    private final BackendService backendService;
    private final Retrier retrier;

    DefaultConsistencyHandler(BackendService backendService, Retrier retrier) {
        this.backendService = backendService;
        this.retrier = retrier;

        loadConsistent();
    }

    @Override
    public boolean isConsistent() {
        return consistent;
    }

    @Override
    public void setConsistent(boolean consistency) throws RetrierGiveUpException {
        this.consistent = consistency;
        flushConsistentState();
        LOGGER.info("Consistent state has been set to '" + consistent + "'");
    }

    private void loadConsistent() {
        try (BackendConnection conn = backendService.openConnection();
                BackendTransaction trans = conn.openReadOnlyTransaction()) {
            Optional<KVValue<?>> valueOpt = trans.readMetaInfo(CONSISTENCY_KEY);
            if (!valueOpt.isPresent()) {
                consistent = false;
                return;
            }
            KVValue<?> value = valueOpt.get();
            if (!value.getType().equals(BooleanType.INSTANCE)) {
                throw new IllegalStateException("Unexpected consistency value "
                        + "found. Expected a boolean but " + valueOpt + " was "
                        + "found");
            }
            consistent = ((KVBoolean) value).getPrimitiveValue();
        }
    }

    private void flushConsistentState() throws RollbackException, RetrierGiveUpException {
        try (BackendConnection conn = backendService.openConnection()) {
            retrier.retry(() -> flushConsistentState(conn));
        }
    }

    private Object flushConsistentState(BackendConnection conn) throws RetrierAbortException {
        try (WriteBackendTransaction trans = conn.openSharedWriteTransaction()) {

            trans.writeMetaInfo(CONSISTENCY_KEY, KVBoolean.from(consistent));
            trans.commit();
        } catch (UserException ex) {
            throw new RetrierAbortException(ex);
        }
        return null;
    }

}
