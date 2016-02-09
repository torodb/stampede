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

package com.torodb.torod.db.backends;

import com.google.common.base.Preconditions;
import com.torodb.torod.core.backend.DbBackend;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import javax.annotation.Nonnull;
import javax.inject.Singleton;
import javax.sql.DataSource;

/**
 *
 */
@Singleton
public abstract class AbstractDbBackend implements DbBackend {
    public static final int SYSTEM_DATABASE_CONNECTIONS = 1;
    public static final int MIN_READ_CONNECTIONS_DATABASE = 1;
    public static final int MIN_SESSION_CONNECTIONS_DATABASE = 2;
    public static final int MIN_CONNECTIONS_DATABASE = SYSTEM_DATABASE_CONNECTIONS + MIN_READ_CONNECTIONS_DATABASE
            + MIN_SESSION_CONNECTIONS_DATABASE;

    private static final int JOB_DEPENDENCY_STRIPES = 16;
    private static final int CACHE_SUB_DOC_TYPE_STRIPES = 64;
    private static final long DEFAULT_CURSOR_TIMEOUT = 10 * 60 * 1000;

    private static final long POOL_CONNECTION_TIMEOUT = 10L * 1000;

    private final DbBackendConfiguration configuration;
    private HikariDataSource commonDataSource;
    private HikariDataSource systemDataSource;
    private HikariDataSource globalCursorDataSource;
    private volatile boolean dataSourcesInitialized = false;

    /**
     * Configure the backend. The contract specifies that any subclass must call initialize() method after
     * properly constructing the object.
     *
     * @param configuration
     */
    public AbstractDbBackend(DbBackendConfiguration configuration) {
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

    protected abstract void setGlobalCursorTransactionIsolation(@Nonnull HikariDataSource dataSource);

    protected synchronized void initialize() {
        if(dataSourcesInitialized) {
            return;
        }

        int reservedReadPoolSize = configuration.getReservedReadPoolSize();

        commonDataSource = createPooledDataSource(
                configuration, "session",
                configuration.getConnectionPoolSize() - reservedReadPoolSize - SYSTEM_DATABASE_CONNECTIONS
        );
        systemDataSource = createPooledDataSource(configuration, "system", SYSTEM_DATABASE_CONNECTIONS);
        globalCursorDataSource = createPooledDataSource(configuration, "cursors", reservedReadPoolSize);
        setGlobalCursorTransactionIsolation(globalCursorDataSource);
        globalCursorDataSource.setReadOnly(true);

        dataSourcesInitialized = true;
    }

    private HikariDataSource createPooledDataSource(
            DbBackendConfiguration configuration, String poolName, int poolSize
    ) {
        HikariConfig hikariConfig = new HikariConfig();

        // Delegate database-specific setting of connection parameters and any specific configuration
        hikariConfig.setDataSource(getConfiguredDataSource(configuration, poolName));

        // Apply ToroDB-specific datasource configuration
        hikariConfig.setConnectionTimeout(POOL_CONNECTION_TIMEOUT);
        hikariConfig.setPoolName(poolName);
        hikariConfig.setMaximumPoolSize(poolSize);
        /*
         * TODO: implement to add metric support. See https://github.com/brettwooldridge/HikariCP/wiki/Codahale-Metrics
         * hikariConfig.setMetricRegistry(...);
         */

        return new HikariDataSource(hikariConfig);
    }

    protected abstract DataSource getConfiguredDataSource(DbBackendConfiguration configuration, String poolName);

    private void checkDataSourcesInitialized() {
        if(! dataSourcesInitialized) {
            initialize();
        }
    }

    @Override
    public DataSource getSessionDataSource() {
        checkDataSourcesInitialized();

        return commonDataSource;
    }

    @Override
    public DataSource getSystemDataSource() {
        checkDataSourcesInitialized();

        return systemDataSource;
    }

    @Override
    public DataSource getGlobalCursorDatasource() {
        checkDataSourcesInitialized();

        return globalCursorDataSource;
    }

    @Override
    public int getByJobDependencyStripes() {
        return JOB_DEPENDENCY_STRIPES;
    }

    @Override
    public int getCacheSubDocTypeStripes() {
        return CACHE_SUB_DOC_TYPE_STRIPES;
    }

    @Override
    public long getDefaultCursorTimeout() {
        return DEFAULT_CURSOR_TIMEOUT;
    }

    @Override
    public void shutdown() {
        commonDataSource.close();
        systemDataSource.close();
        globalCursorDataSource.close();
    }
}
