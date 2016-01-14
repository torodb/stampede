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
package com.torodb.torod.core.cursors;

import com.google.common.collect.FluentIterable;
import com.torodb.torod.core.exceptions.ClosedToroCursorException;
import com.torodb.torod.core.exceptions.ToroImplementationException;
import com.torodb.torod.core.exceptions.UnknownMaxElementsException;
import com.torodb.torod.core.executor.SessionExecutor;
import com.torodb.torod.core.subdocument.ToroDocument;
import javax.annotation.Nonnegative;
import javax.annotation.concurrent.ThreadSafe;

/**
 * This is the internal cursor iterface that is used inside ToroDB to manage
 * db and memory cursors.
 *
 * This class contain some implementation details that the user layers of ToroDB
 * do not need to know.
 *
 * Several users might read from the same ToroCursor, but each request one must
 * use its own {@link UserCursor} instance.
 */
@ThreadSafe
public interface ToroCursor {

    public CursorId getId();
    
    public boolean hasTimeout();
    
    /**
     * Returns true iff the cursor has a limit.
     * 
     * A cursor has a limit if that limit has been defined at creation time.
     * The value of this limit can be obtained with {@linkplain #getLimit() }.
     * If that number (or more) objects are read, then the cursor is exhausted.
     * An exhausted cursor will be closed automatically iff it is also
     * {@linkplain #isAutoclosable() autoclosable}
     * 
     * @return true iff the cursor has a limit
     */
    public boolean hasLimit();
    /**
     * 
     * @return the maximum number of elements this cursor will return
     * @throws ToroImplementationException if {@link #hasLimit() } returns true
     */
    @Nonnegative
    public int getLimit() throws ToroImplementationException;
    /**
     * Returns true iff this cursor is autoclosable.
     * 
     * An autoclosable cursor will be automatically closed when it is 
     * {@linkplain #hasLimit() exhausted} or when there are no more objects
     * that fulfill the query.
     * 
     * @return true iff this cursor is autoclosable
     */
    public boolean isAutoclosable();
    
    /**
     * Read all object pointed by this cursor.
     * 
     * If the cursor is {@linkplain #isAutoclosable() autoclosable}, then the
     * cursor will be closed.
     * 
     * @param executor the executor that will execute the action
     * @return 
     * @throws com.torodb.torod.core.exceptions.ClosedToroCursorException 
     */
    public FluentIterable<ToroDocument> readAll(SessionExecutor executor) throws ClosedToroCursorException;
    
    /**
     * Read a maximun number of documents from this cursor.
     * 
     * The maximum size of the returned list is the given limit, but less
     * documents can be send if the cursor does not have enough documents.
     * 
     * If the cursor is {@linkplain #isAutoclosable() autoclosable} and all
     * documents are read, then the cursor will be closed.
     * 
     * @param executor the executor that will execute the action
     * @param limit a positive integer (>= 1)
     * @return
     * @throws com.torodb.torod.core.exceptions.ClosedToroCursorException
     */
    public FluentIterable<ToroDocument> read(SessionExecutor executor, @Nonnegative int limit) throws ClosedToroCursorException;
    
    /**
     * Close the cursor, releasing the resources associated with it.
     * 
     * This method do not return until the crsor is closed
     * @param executor the executor that will execute the action
     */
    public void close(SessionExecutor executor);

    public boolean isClosed();
    
    /**
     * 
     * @param executor the executor that will execute the action
     * @return The number of objects that have been already read
     * @throws com.torodb.torod.core.exceptions.ClosedToroCursorException
     */
    public int getPosition(SessionExecutor executor) throws ClosedToroCursorException;
    
    /**
     * Returns the maximum number of documents this cursor is going to return or
     * throws an exception if this number cannot be known without blocking the
     * thread.
     * 
     * The maximum number of documents is the minimum between {@linkplain #getLimit() }
     * (if {@linkplain #hasLimit() ) is true) and the number of elements 
     * iterated with the cursor.
     * 
     * This value should be constant over time.
     * 
     * @return 
     * @throws UnknownMaxElementsException
     */
    public int getMaxElements() throws UnknownMaxElementsException;
}
