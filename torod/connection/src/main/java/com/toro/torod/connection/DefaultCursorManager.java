
package com.toro.torod.connection;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.torodb.torod.core.connection.CursorManager;
import com.torodb.torod.core.cursors.CursorId;
import com.torodb.torod.core.cursors.CursorProperties;
import com.torodb.torod.core.cursors.InnerCursorManager;
import com.torodb.torod.core.d2r.D2RTranslator;
import com.torodb.torod.core.dbWrapper.DbWrapper;
import com.torodb.torod.core.executor.SessionExecutor;
import com.torodb.torod.core.executor.ToroTaskExecutionException;
import com.torodb.torod.core.language.projection.Projection;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.subdocument.SplitDocument;
import com.torodb.torod.core.subdocument.ToroDocument;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 *
 */
public class DefaultCursorManager implements CursorManager {

    private final InnerCursorManager innerCursorManager;
    private final DbWrapper dbWrapper;
    private final SessionExecutor sessionExecutor;
    private final D2RTranslator d2r;

    @Inject
    public DefaultCursorManager(
            InnerCursorManager innerCursorManager, 
            DbWrapper dbWrapper, 
            SessionExecutor sessionExecutor, 
            D2RTranslator d2r) {
        this.innerCursorManager = innerCursorManager;
        this.dbWrapper = dbWrapper;
        this.sessionExecutor = sessionExecutor;
        this.d2r = d2r;
    }

    @Override
    public CursorId openUnlimitedCursor(
            String collection,
            @Nullable QueryCriteria queryCriteria,
            @Nullable Projection projection,
            int numberToSkip,
            boolean autoclose,
            boolean hasTimeout) {

        CursorProperties cursor = innerCursorManager.openUnlimitedCursor(
                        hasTimeout,
                        autoclose
                );
        sessionExecutor.query(
                collection,
                cursor.getId(),
                queryCriteria,
                projection,
                0
        );

        return cursor.getId();
    }

    @Override
    public CursorId openLimitedCursor(
            String collection,
            @Nullable QueryCriteria queryCriteria,
            @Nullable Projection projection,
            int numberToSkip,
            int maxResults,
            boolean autoclose,
            boolean hasTimeout) {

        if (maxResults <= 0) {
            throw new IllegalArgumentException(
                    "The limit must be higher than 0 (>= 1)");
        }
        CursorProperties cursor = innerCursorManager.openLimitedCursor(
                        hasTimeout,
                        autoclose,
                        maxResults
                );
        sessionExecutor.query(
                collection, 
                cursor.getId(),
                queryCriteria,
                projection,
                maxResults
        );

        return cursor.getId();
    }

    @Override
    public List<ToroDocument> readCursor(CursorId cursorId,
                                         int limit) {
        //TODO: check security

        if (limit <= 0) {
            throw new IllegalArgumentException(
                    "Limit must be a positive numbre, but " + limit
                    + " was recived");
        }

        try {
            List<? extends SplitDocument> splitDocs = sessionExecutor
                    .readCursor(cursorId, limit)
                    .get();
            List<ToroDocument> docs = Lists.newArrayListWithCapacity(
                    splitDocs.size()
            );
            for (SplitDocument splitDocument : splitDocs) {
                docs.add(d2r.translate(sessionExecutor, splitDocument));
            }

            innerCursorManager.notifyRead(cursorId, docs.size());

            return docs;
        }
        catch (ToroTaskExecutionException ex) {
            //TODO: Change exceptions
            throw new RuntimeException(ex);
        }
        catch (InterruptedException ex) {
            //TODO: Change exceptions
            throw new RuntimeException(ex);
        }
        catch (ExecutionException ex) {
            //TODO: Change exceptions
            throw new RuntimeException(ex);
        }
    }

    @Override
    public List<ToroDocument> readAllCursor(CursorId cursorId) {
        //TODO: check security

        try {
            List<? extends SplitDocument> splitDocs;

            CursorProperties prop = innerCursorManager.getCursor(cursorId);
            if (prop.hasLimit()) {
                int elementsToRead = prop.getLimit() - innerCursorManager.
                        getReadElements(cursorId);

                assert elementsToRead >= 0;

                if (elementsToRead == 0) {
                    return Collections.emptyList();
                }
                else {
                    splitDocs = sessionExecutor.readAllCursor(cursorId).get();
                }
            }
            else {
                splitDocs = sessionExecutor.readAllCursor(cursorId).get();
            }

            List<ToroDocument> docs = Lists.newArrayListWithCapacity(splitDocs.
                    size());
            for (SplitDocument splitDocument : splitDocs) {
                docs.add(d2r.translate(sessionExecutor, splitDocument));
            }

            innerCursorManager.notifyAllRead(cursorId);

            return docs;
        }
        catch (ToroTaskExecutionException ex) {
            //TODO: Change exceptions
            throw new RuntimeException(ex);
        }
        catch (InterruptedException ex) {
            //TODO: Change exceptions
            throw new RuntimeException(ex);
        }
        catch (ExecutionException ex) {
            //TODO: Change exceptions
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Future<Integer> getPosition(CursorId cursorId) {
        return Futures.immediateFuture(innerCursorManager.getReadElements(cursorId));
    }

    @Override
    public Future<Integer> countRemainingDocs(CursorId cursorId) {
        return sessionExecutor.countRemainingDocs(cursorId);
    }

    @Override
    public void closeCursor(CursorId cursorId) {
        //TODO: check security

        //when a cursor is removed from the cursor manager, it is closed in the database
        innerCursorManager.close(cursorId);
    }
}
