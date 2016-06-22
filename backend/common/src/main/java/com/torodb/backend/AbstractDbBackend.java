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
import com.google.common.util.concurrent.AbstractIdleService;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import javax.annotation.Nonnull;
import javax.inject.Singleton;
import javax.sql.DataSource;

/**
 *
 */
@Singleton
public abstract class AbstractDbBackend<Configuration extends DbBackendConfiguration> extends AbstractIdleService implements DbBackend {
    public static final int SYSTEM_DATABASE_CONNECTIONS = 1;
    public static final int MIN_READ_CONNECTIONS_DATABASE = 1;
    public static final int MIN_SESSION_CONNECTIONS_DATABASE = 2;
    public static final int MIN_CONNECTIONS_DATABASE = SYSTEM_DATABASE_CONNECTIONS + MIN_READ_CONNECTIONS_DATABASE
            + MIN_SESSION_CONNECTIONS_DATABASE;

    private final Configuration configuration;
    private HikariDataSource commonDataSource;
    private HikariDataSource systemDataSource;
    private HikariDataSource globalCursorDataSource;

    /**
     * Configure the backend. The contract specifies that any subclass must call initialize() method after
     * properly constructing the object.
     *
     * @param configuration
     */
    public AbstractDbBackend(Configuration configuration) {
        this.configuration = configuration;

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

        commonDataSource = createPooledDataSource(
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
        globalCursorDataSource = createPooledDataSource(
                configuration, "cursors",
                reservedReadPoolSize,
                getGlobalCursorTransactionIsolation(),
                true);
    }

    @Override
    protected void shutDown() throws Exception {
        commonDataSource.close();
        systemDataSource.close();
        globalCursorDataSource.close();
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
    public DataSource getSessionDataSource() {
        Preconditions.checkState(isRunning(), "The " + DbBackend.class + " is not running");

        return commonDataSource;
    }

    @Override
    public DataSource getSystemDataSource() {
        Preconditions.checkState(isRunning(), "The " + DbBackend.class + " is not running");

        return systemDataSource;
    }

    @Override
    public DataSource getGlobalCursorDatasource() {
        Preconditions.checkState(isRunning(), "The " + DbBackend.class + " is not running");

        return globalCursorDataSource;
    }

    @Override
    public long getDefaultCursorTimeout() {
        return configuration.getCursorTimeout();
    }
}
