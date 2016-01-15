/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */
package com.toro.torod.connection.cursors;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.torodb.torod.core.cursors.CursorId;
import com.torodb.torod.core.d2r.D2RTranslator;
import com.torodb.torod.core.exceptions.ClosedToroCursorException;
import com.torodb.torod.core.exceptions.NotAutoclosableCursorException;
import com.torodb.torod.core.exceptions.ToroRuntimeException;
import com.torodb.torod.core.exceptions.UnknownMaxElementsException;
import com.torodb.torod.core.executor.SessionExecutor;
import com.torodb.torod.core.executor.ToroTaskExecutionException;
import com.torodb.torod.core.subdocument.SplitDocument;
import com.torodb.torod.core.subdocument.ToroDocument;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class StandardDocumentToroCursor extends DefaultToroCursor {

    private static final Logger LOGGER
            = LoggerFactory.getLogger(StandardDocumentToroCursor.class);
    private final D2RTranslator d2r;
    private int position;
    private final int maxElements;

    public StandardDocumentToroCursor(
            SessionExecutor executor,
            CursorId id, 
            boolean hasTimeout, 
            int limit, 
            boolean autoclosable,
            D2RTranslator d2r) {
        super(id, hasTimeout, limit, autoclosable);
        this.d2r = d2r;
        this.position = 0;
        int maxElementsCopy;
        try {
            maxElementsCopy = executor.getMaxElements(id).get();
        }
        catch (InterruptedException ex) {
            throw new ToroRuntimeException(ex);
        }
        catch (ExecutionException ex) {
            if (ex.getCause() instanceof UnknownMaxElementsException) {
                maxElementsCopy = limit;
            }
            else {
                throw new ToroRuntimeException(ex);
            }
        }
        maxElements = Math.min(maxElementsCopy, limit);
    }

    public StandardDocumentToroCursor(
            SessionExecutor executor,
            CursorId id, 
            boolean hasTimeout, 
            boolean autoclosable,
            D2RTranslator d2r) throws NotAutoclosableCursorException {
        super(id, hasTimeout, autoclosable);
        this.d2r = d2r;
        this.position = 0;
        int maxElementsCopy;
        try {
            maxElementsCopy = executor.getMaxElements(id).get();
        }
        catch (InterruptedException ex) {
            throw new ToroRuntimeException(ex);
        }
        catch (ExecutionException ex) {
            if (ex.getCause() instanceof UnknownMaxElementsException) {
                maxElementsCopy = -1;
                if (autoclosable) {
                    throw new NotAutoclosableCursorException();
                }
            }
            else {
                throw new ToroRuntimeException(ex);
            }
        }
        maxElements = maxElementsCopy;
    }
    
    @Override
    public FluentIterable<ToroDocument> readAll(SessionExecutor executor) throws ClosedToroCursorException {
        try {
            executor.noop().get();
            
            synchronized (this) {
                if (isClosed()) {
                    throw new ClosedToroCursorException();
                }
                
                List<? extends SplitDocument> splitDocs = executor
                        .readAllCursor(getId())
                        .get();

                position += splitDocs.size();
                
                if (isAutoclosable() && position == maxElements) {
                    close(executor);
                }

                return FluentIterable.from(
                        Iterables.transform(
                                splitDocs,
                                d2r.getToDocumentFunction()
                        )
                );
            }
        }
        catch (ToroTaskExecutionException | InterruptedException | ExecutionException ex) {
            //TODO: Change exceptions
            throw new ToroRuntimeException(ex);
        }
    }

    @Override
    public FluentIterable<ToroDocument> read(SessionExecutor executor, int limit) throws ClosedToroCursorException {
        if (limit <= 0) {
            throw new IllegalArgumentException(
                    "Limit must be a positive numbre, but " + limit
                    + " was recived");
        }

        try {
            executor.noop().get();
            
            synchronized (this) {
                if (isClosed()) {
                    throw new ClosedToroCursorException();
                }
                limit = Math.min(limit, maxElements - position);

                FluentIterable<ToroDocument> docs;
                if (limit > 0) {

                    List<? extends SplitDocument> splitDocs = executor
                            .readCursor(getId(), limit)
                            .get();
                    position += splitDocs.size();

                    docs = FluentIterable.from(
                            Iterables.transform(
                                    splitDocs,
                                    d2r.getToDocumentFunction()
                            )
                    );
                }
                else {
                    docs = FluentIterable.from(Collections.<ToroDocument>emptyList());
                }
                
                if (isAutoclosable() && position == maxElements) {
                    close(executor);
                }
                
                return docs;
            }
        }
        catch (ToroTaskExecutionException | InterruptedException | ExecutionException ex) {
            //TODO: Change exceptions
            throw new ToroRuntimeException(ex);
        }
    }

    @Override
    public void closeImmediately(SessionExecutor executor) {
        try {
            executor.closeCursor(getId());
        }
        catch (ToroTaskExecutionException ex) {
            throw new ToroRuntimeException(ex);
        }
    }

    @Override
    public int getPosition(SessionExecutor executor) {
        try {
            executor.noop().get();
            synchronized (this) {
                return position;
            }
        }
        catch (InterruptedException ex) {
            throw new ToroRuntimeException(ex);
        }
        catch (ExecutionException ex) {
            throw new ToroRuntimeException(ex);
        }
    }

    @Override
    public int getMaxElements() throws UnknownMaxElementsException {
        if (maxElements < 0) {
            throw new UnknownMaxElementsException();
        }
        return maxElements;
    }
   
}
