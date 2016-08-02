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

package com.torodb.backend;

import com.google.common.base.Preconditions;
import com.torodb.backend.ErrorHandler.Context;
import com.torodb.common.util.ThreadFactoryIdleService;
import com.torodb.core.annotations.ToroDbIdleService;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ThreadFactory;
import javax.annotation.Nonnull;
import javax.inject.Singleton;
import javax.sql.DataSource;

/**
 *
 */
@Singleton
public abstract class AbstractDbBackend<Configuration extends DbBackendConfiguration> 
        extends ThreadFactoryIdleService implements DbBackendService {
    public static final int SYSTEM_DATABASE_CONNECTIONS = 1;
    public static final int MIN_READ_CONNECTIONS_DATABASE = 1;
    public static final int MIN_SESSION_CONNECTIONS_DATABASE = 2;
    public static final int MIN_CONNECTIONS_DATABASE = SYSTEM_DATABASE_CONNECTIONS + MIN_READ_CONNECTIONS_DATABASE
            + MIN_SESSION_CONNECTIONS_DATABASE;

    private final Configuration configuration;
    private final ErrorHandler errorHandler;
    private HikariDataSource writeDataSource;
    private HikariDataSource systemDataSource;
    private HikariDataSource readOnlyDataSource;
    /**
     * Global state variable that indicate if internal indexes have to be created or not
     */
    private volatile boolean includeInternalIndexes;

    /**
     * Configure the backend. The contract specifies that any subclass must call initialize() method after
     * properly constructing the object.
     *
     * @param threadFactory the thread factory that will be used to create the startup and shutdown
     *                      threads
     * @param configuration
     */
    public AbstractDbBackend(@ToroDbIdleService ThreadFactory threadFactory, 
            Configuration configuration, ErrorHandler errorHandler) {
        super(threadFactory);
        this.configuration = configuration;
        this.errorHandler = errorHandler;
        this.includeInternalIndexes = true;

        int connectionPoolSize = configuration.getConnectionPoolSize();
        int reservedReadPoolSize = configuration.getReservedReadPoolSize();
        Preconditions.checkState(
                connectionPoolSize >= MIN_CONNECTIONS_DATABASE,
                "At least " + MIN_CONNECTIONS_DATABASE + " total connections with the backend SQL database are required"
        );
        Preconditions.checkState(
                reservedReadPoolSize >= MIN_READ_CONNECTIONS_DATABASE,
                "At least " + MIN_READ_CONNECTIONS_DATABASE + " read connection(s) is(are) required"
        );
        Preconditions.checkState(
                connectionPoolSize - reservedReadPoolSize >= MIN_SESSION_CONNECTIONS_DATABASE,
                "Reserved read connections must be lower than total connections minus "
                        + MIN_SESSION_CONNECTIONS_DATABASE
        );
    }

    @Override
    protected void startUp() throws Exception {
        int reservedReadPoolSize = configuration.getReservedReadPoolSize();

        writeDataSource = createPooledDataSource(
                configuration, "session",
                configuration.getConnectionPoolSize() - reservedReadPoolSize - SYSTEM_DATABASE_CONNECTIONS,
                getCommonTransactionIsolation(),
                false
        );
        systemDataSource = createPooledDataSource(
                configuration, "system",
                SYSTEM_DATABASE_CONNECTIONS,
                getSystemTransactionIsolation(),
                false);
        readOnlyDataSource = createPooledDataSource(
                configuration, "cursors",
                reservedReadPoolSize,
                getGlobalCursorTransactionIsolation(),
                true);
    }

    @Override
    @SuppressFBWarnings(value = "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR",
    justification = "Object lifecyle is managed as a Service. Datasources are initialized in setup method")
    protected void shutDown() throws Exception {
        writeDataSource.close();
        systemDataSource.close();
        readOnlyDataSource.close();
    }

    @Nonnull
    protected abstract TransactionIsolationLevel getCommonTransactionIsolation();

    @Nonnull
    protected abstract TransactionIsolationLevel getSystemTransactionIsolation();

    @Nonnull
    protected abstract TransactionIsolationLevel getGlobalCursorTransactionIsolation();

    private HikariDataSource createPooledDataSource(
            Configuration configuration, String poolName, int poolSize,
            TransactionIsolationLevel transactionIsolationLevel,
            boolean readOnly
    ) {
        HikariConfig hikariConfig = new HikariConfig();

        // Delegate database-specific setting of connection parameters and any specific configuration
        hikariConfig.setDataSource(getConfiguredDataSource(configuration, poolName));

        // Apply ToroDB-specific datasource configuration
        hikariConfig.setConnectionTimeout(configuration.getConnectionPoolTimeout());
        hikariConfig.setPoolName(poolName);
        hikariConfig.setMaximumPoolSize(poolSize);
        hikariConfig.setTransactionIsolation(transactionIsolationLevel.name());
        hikariConfig.setReadOnly(readOnly);
        /*
         * TODO: implement to add metric support. See https://github.com/brettwooldridge/HikariCP/wiki/Codahale-Metrics
         * hikariConfig.setMetricRegistry(...);
         */

        return new HikariDataSource(hikariConfig);
    }

    protected abstract DataSource getConfiguredDataSource(Configuration configuration, String poolName);
    
    @Override
    public void enableInternalIndexes() {
        this.includeInternalIndexes = true;
    }
    
    @Override
    public void disableInternalIndexes() {
        this.includeInternalIndexes = false;
    }

    @Override
    public DataSource getSessionDataSource() {
        checkState();

        return writeDataSource;
    }

    @Override
    public DataSource getSystemDataSource() {
        checkState();

        return systemDataSource;
    }

    @Override
    public DataSource getGlobalCursorDatasource() {
        checkState();

        return readOnlyDataSource;
    }

    protected void checkState() {
        if (!isRunning()) {
            throw new IllegalStateException("The " + DbBackend.class + " is not running");
        }
    }

    @Override
    public long getDefaultCursorTimeout() {
        return configuration.getCursorTimeout();
    }
    
    @Override
    public boolean includeInternalIndexes() {
        return includeInternalIndexes;
    }
    
    @Override
    public boolean includeForeignKeys() {
        return configuration.includeForeignKeys();
    }

    protected void postConsume(Connection connection, boolean readOnly) throws SQLException {
        connection.setReadOnly(readOnly);
        if (!connection.isValid(500)) {
            throw new RuntimeException("DB connection is not valid");
        }
        connection.setAutoCommit(false);
    }
    
    private Connection consumeConnection(DataSource ds, boolean readOnly) {
        checkState();

        try {
            Connection c = ds.getConnection();
            postConsume(c, readOnly);

            return c;
        } catch (SQLException ex) {
            throw errorHandler.handleException(Context.GET_CONNECTION, ex);
        }
    }

    @Override
    public Connection createSystemConnection() {
        checkState();

        return consumeConnection(systemDataSource, false);
    }

    @Override
    public Connection createReadOnlyConnection() {
        checkState();

        return consumeConnection(readOnlyDataSource, true);
    }

    @Override
    public Connection createWriteConnection() {
        checkState();

        return consumeConnection(writeDataSource, false);
    }
}
