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

package com.torodb.torod.db.sql;

import com.torodb.torod.db.postgresql.meta.TorodbMeta;
import com.torodb.torod.core.config.TorodConfig;
import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.dbWrapper.DbWrapper;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.inject.Inject;
import javax.sql.DataSource;
import org.jooq.Configuration;
import org.jooq.ConnectionProvider;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

/**
 *
 */
public abstract class AbstractSqlDbWrapper implements DbWrapper {

    
	private final AtomicBoolean isInitialized;
    private final DataSource sessionDataSource;
    private final DataSource systemDataSource;
    private TorodbMeta meta;
    

    @Inject
    public AbstractSqlDbWrapper(TorodConfig config) {
        this.sessionDataSource = config.getSessionDataSource();
        this.systemDataSource = config.getSystemDataSource();

        isInitialized = new AtomicBoolean(false);
    }

    protected abstract Configuration getJooqConfiguration(ConnectionProvider connectionProvider);

    protected abstract DbConnection reserveConnection(DSLContext dsl, TorodbMeta meta);

    private DSLContext getDsl(Connection c) {
        return DSL.using(getJooqConfiguration(new MyConnectionProvider(c)));
    }

    protected abstract void checkDbSupported(Connection c) throws SQLException, ImplementationDbException;
    
    @Override
    public void initialize() throws ImplementationDbException {
        if (isInitialized()) {
            throw new IllegalStateException("The db-wrapper is already initialized");
        }

        Connection c = null;

        try {
            c = sessionDataSource.getConnection();
            checkDbSupported(c);
            c.setAutoCommit(false);
            
            meta = new TorodbMeta(getDsl(c));
            c.commit();

            isInitialized.set(true);
        } catch (IOException ex) {
            //TODO: Change exception
            throw new RuntimeException(ex);
        } catch (SQLException ex) {
            //TODO: Change exception
            throw new RuntimeException(ex);
        } catch (DataAccessException ex) {
            //TODO: Change exception
            throw new RuntimeException(ex);
        } finally {
            try {
                if (c != null) {
                    c.close();
                }
            } catch (SQLException ex) {
            }
        }
    }

    @Override
    public DbConnection consumeSessionDbConnection() {
        return consumeConnection(sessionDataSource);
    }

    @Override
    public DbConnection getSystemDbConnection() throws ImplementationDbException {
        return consumeConnection(systemDataSource);
    }
    
    private DbConnection consumeConnection(DataSource ds) {
        if (!isInitialized()) {
            throw new IllegalStateException("The db-wrapper is not initialized");
        }
        Connection c = null;
        try {
            c = ds.getConnection();
            c.setAutoCommit(false);
            //TODO: Set isolation

            return reserveConnection(getDsl(c), meta);
        } catch (SQLException ex) {
            if (c != null) {
                try {
                    c.close();
                } catch (SQLException ex1) {
                }
            }
            //TODO: Change exception
            throw new RuntimeException(ex);
        }
    }
    
    
    public boolean isInitialized() {
        return isInitialized.get();
    }

    public static class MyConnectionProvider implements ConnectionProvider {

        private final Connection connection;

        public MyConnectionProvider(Connection connection) {
            this.connection = connection;
        }

        @Override
        public Connection acquire() throws DataAccessException {
            return connection;
        }

        @Override
        public void release(Connection connection) throws DataAccessException {
        }

    }

}
