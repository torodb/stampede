
package com.toro.torod.connection;

import com.toro.torod.connection.cursors.StandardDocumentToroCursor;
import com.torodb.torod.core.backend.DbBackend;
import com.torodb.torod.core.cursors.CursorId;
import com.torodb.torod.core.cursors.ToroCursor;
import com.torodb.torod.core.cursors.ToroCursorManager;
import com.torodb.torod.core.d2r.D2RTranslator;
import com.torodb.torod.core.exceptions.CursorNotFoundException;
import com.torodb.torod.core.exceptions.NotAutoclosableCursorException;
import com.torodb.torod.core.exceptions.ToroRuntimeException;
import com.torodb.torod.core.executor.SessionExecutor;
import com.torodb.torod.core.language.projection.Projection;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 *
 */
public class DefaultCursorManager implements ToroCursorManager {

    private final ToroCursorStorage storage;
    private final D2RTranslator d2r;
    private final AtomicInteger idProvider;

    @Inject
    public DefaultCursorManager(
            DbBackend backend,
            D2RTranslator d2r) {
        this.storage = new ToroCursorStorage(backend);
        this.d2r = d2r;
        this.idProvider = new AtomicInteger(0);
    }
    
    @Override
    public ToroCursor openUnlimitedCursor(
            @Nonnull SessionExecutor sessionExecutor,
            String collection,
            @Nullable QueryCriteria queryCriteria,
            @Nullable Projection projection,
            int numberToSkip,
            boolean autoclose,
            boolean hasTimeout) throws NotAutoclosableCursorException {

        CursorId id = consumeId();
        
        Future<Void> query = sessionExecutor.query(
                collection,
                id,
                queryCriteria,
                projection,
                0
        );
        try {
            query.get();
        }
        catch (InterruptedException | ExecutionException ex) {
            throw new ToroRuntimeException(ex);
        }
        
        StandardDocumentToroCursor cursor;
        cursor = new StandardDocumentToroCursor(
                sessionExecutor,
                id,
                hasTimeout,
                autoclose,
                d2r
        );
        return storage.storeCursor(cursor, sessionExecutor);
    }

    @Override
    public ToroCursor openLimitedCursor(
            @Nonnull SessionExecutor sessionExecutor,
            String collection,
            @Nullable QueryCriteria queryCriteria,
            @Nullable Projection projection,
            int numberToSkip,
            int maxResults,
            boolean autoclose,
            boolean hasTimeout) {

        CursorId id = consumeId();
        
        Future<Void> query = sessionExecutor.query(
                collection,
                id,
                queryCriteria,
                projection,
                maxResults
        );
        try {
            query.get();
        }
        catch (InterruptedException | ExecutionException ex) {
            throw new ToroRuntimeException(ex);
        }
        
        StandardDocumentToroCursor cursor = new StandardDocumentToroCursor(
                sessionExecutor,
                id, 
                hasTimeout, 
                maxResults,
                autoclose, 
                d2r
        );
        return storage.storeCursor(cursor, sessionExecutor);
    }

    @Override
    public ToroCursor lookForCursor(CursorId cursorId) throws CursorNotFoundException {
        ToroCursor cursor = storage.getCursor(cursorId);
        return cursor;
    }
    
    private CursorId consumeId() {
        return new CursorId(idProvider.incrementAndGet());
    }
}
