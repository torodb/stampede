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
package com.torodb.torod.core.backend;

import com.torodb.torod.core.Backend;

import javax.sql.DataSource;

/**
 * A specialized Backend based on an underlying database service to perform all data operations.
 */
public interface DbBackend extends Backend {
    public DataSource getSessionDataSource();

    /**
     * Returns a datasource that provides at least one connection.
     * <p>
     * This will be the connection where all system actions (like create tables
     * or reserve ids) will be executed.
     * <p>
     * To avoid interblocks, this datasource must be able to provide a
     * connection even if the
     * {@link #getSessionDataSource() common datasource} does not have any
     * free connection.
     * <p>
     * No more than one system connection is used at the same time, so a 
     * datasource with only one connection perfectly fits with this datasource.
     * @return
     */
    public DataSource getSystemDataSource();
    
    public DataSource getGlobalCursorDatasource();

    public int getByJobDependencyStripes();

    public int getCacheSubDocTypeStripes();

    public long getDefaultCursorTimeout();

    public void shutdown();
}
