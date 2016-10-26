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

package com.torodb.backend.postgresql;


import com.torodb.backend.AbstractDbBackendService;
import com.torodb.backend.TransactionIsolationLevel;
import com.torodb.backend.driver.postgresql.PostgreSQLDbBackendConfiguration;
import com.torodb.backend.driver.postgresql.PostgreSQLDriverProvider;
import java.util.concurrent.ThreadFactory;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.sql.DataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.torodb.core.annotations.TorodbIdleService;

/**
 *
 * PostgreSQL-based backend
 */
public class PostgreSQLDbBackend extends AbstractDbBackendService<PostgreSQLDbBackendConfiguration> {

    private static final Logger LOGGER = LogManager.getLogger(PostgreSQLDbBackend.class);

    private final PostgreSQLDriverProvider driverProvider;

    @Inject
    public PostgreSQLDbBackend(@TorodbIdleService ThreadFactory threadFactory,
            PostgreSQLDbBackendConfiguration configuration, 
            PostgreSQLDriverProvider driverProvider,
            PostgreSQLErrorHandler errorHandler) {
        super(threadFactory, configuration, errorHandler);

        LOGGER.info("Configured PostgreSQL backend at {}:{}", configuration.getDbHost(), configuration.getDbPort());

        this.driverProvider = driverProvider;
    }

    @Override
    protected DataSource getConfiguredDataSource(PostgreSQLDbBackendConfiguration configuration, String poolName) {
        return driverProvider.getConfiguredDataSource(configuration, poolName);
    }

    @Override
    @Nonnull
    protected TransactionIsolationLevel getCommonTransactionIsolation() {
        return TransactionIsolationLevel.TRANSACTION_REPEATABLE_READ;
    }

    @Override
    @Nonnull
    protected TransactionIsolationLevel getSystemTransactionIsolation() {
        return TransactionIsolationLevel.TRANSACTION_REPEATABLE_READ;
    }

    @Override
    @Nonnull
    protected TransactionIsolationLevel getGlobalCursorTransactionIsolation() {
        return TransactionIsolationLevel.TRANSACTION_REPEATABLE_READ;
    }
}
