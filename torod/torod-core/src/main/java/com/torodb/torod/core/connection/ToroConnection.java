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

package com.torodb.torod.core.connection;

import com.torodb.torod.core.pojos.Database;
import com.torodb.torod.core.Session;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import java.io.Closeable;
import java.util.List;
import java.util.concurrent.Future;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;

/**
 *
 */
@NotThreadSafe
public interface ToroConnection extends Closeable {
    
    /**
     * Close the connection and rollback its changes since last commit.
     */
    @Override
    public void close();
    
    public ToroTransaction createTransaction() throws ImplementationDbException;
    
    public CursorManager getCursorManager();
    
    public Session getSession();

    /**
     * Checks whether a collection already exists. If it doesn't, it creates the collection
     * @param collection The name of the collection
     * @return true if the collection was created, false if it already existed
     */
    public boolean createCollection(String collection);

    
    public Future<List<? extends Database>> getDatabases();
    
}
