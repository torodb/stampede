
package com.toro.torod.connection;

import com.toro.torod.connection.cursors.CollectionMetainfoToroCursor;
import com.toro.torod.connection.cursors.StandardDocumentToroCursor;
import com.torodb.torod.core.backend.DbBackend;
import com.torodb.torod.core.cursors.*;
import com.torodb.torod.core.d2r.D2RTranslator;
import com.torodb.torod.core.exceptions.CursorNotFoundException;
import com.torodb.torod.core.exceptions.NotAutoclosableCursorException;
import com.torodb.torod.core.exceptions.ToroRuntimeException;
import com.torodb.torod.core.executor.SessionExecutor;
import com.torodb.torod.core.language.projection.Projection;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.pojos.CollectionMetainfo;
import com.torodb.torod.core.subdocument.ToroDocument;
import java.util.List;
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
    public ToroCursor<ToroDocument> openUnlimitedCursor(
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
        catch (InterruptedException ex) {
            throw new ToroRuntimeException(ex);
        }
        catch (ExecutionException ex) {
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
    public ToroCursor<ToroDocument> openLimitedCursor(
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
        catch (InterruptedException ex) {
            throw new ToroRuntimeException(ex);
        }
        catch (ExecutionException ex) {
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
    public ToroCursor<CollectionMetainfo> openCollectionsMetainfoCursor(
            @Nonnull SessionExecutor sessionExecutor) {
        Future<List<CollectionMetainfo>> futureMetainfo = 
                sessionExecutor.getCollectionsMetainfo();
        
        CursorId id = consumeId();
        CollectionMetainfoToroCursor cursor = new CollectionMetainfoToroCursor(
                id, 
                true, 
                true, 
                futureMetainfo
        );
        
        return storage.storeCursor(cursor, sessionExecutor);
    }

    @Override
    public ToroCursor lookForCursor(CursorId cursorId) throws
            CursorNotFoundException {
        return storage.getCursor(cursorId);
    }

    @Override
    public <E> ToroCursor<E> lookForCursor(CursorId cursorId, Class<E> expectedTypeClass)
            throws CursorNotFoundException {
        ToroCursor cursor = storage.getCursor(cursorId);
        if (expectedTypeClass.isAssignableFrom(cursor.getType())) {
            return cursor;
        }
        throw new CursorNotFoundException(
                cursorId, 
                "Cursor with "+ cursorId+" is of a different type ("+cursor.getType()+")"
        );
    }
    
    private CursorId consumeId() {
        return new CursorId(idProvider.incrementAndGet());
    }
}
