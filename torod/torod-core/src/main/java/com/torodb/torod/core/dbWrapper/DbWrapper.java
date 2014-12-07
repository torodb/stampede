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
package com.torodb.torod.core.dbWrapper;

import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import javax.annotation.Nonnull;

/**
 *
 */
public interface DbWrapper {

    /**
     * Initializes this object.
     * <p>
     * This method is called when ToroD starts to retrieve some internal
     * information from the database and setup inner data structures.
     * @throws ImplementationDbException upon detecting an incompatible version of the database.
     */
    public void initialize() throws ImplementationDbException;

    /**
     * Returns a {@link DbConnection}.
     * <p>
     * Caller owns the connection and he must call {@link DbConnection#close()}
     * to release it.
     * <p>
     * The calling thread will be blocked until a SQL connection is usable if
     * the connection pooler is blocker.
     * <p>
     * @return @throws
     * com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException
     */
    @Nonnull
    public DbConnection consumeSessionDbConnection() throws ImplementationDbException;

    /**
     * Retruns the {@link DbConnection} that is reserved to execute system
     * actions.
     * <p>
     * Only a system thread should use this connection.
     * <p>
     * @return
     * @throws ImplementationDbException
     */
    public DbConnection getSystemDbConnection() throws ImplementationDbException;

}
