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

package com.torodb.torod.db.backends.postgresql;


import com.torodb.torod.db.backends.AbstractDbBackend;
import com.torodb.torod.db.backends.DbBackendConfiguration;
import com.torodb.torod.backends.drivers.postgresql.PostgreSQLDriverProvider;
import com.zaxxer.hikari.HikariDataSource;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.sql.DataSource;

/**
 *
 * PostgreSQL-based backend
 */
public class PostgreSQLDbBackend extends AbstractDbBackend {
    private final PostgreSQLDriverProvider driverProvider;

    @Inject
    public PostgreSQLDbBackend(DbBackendConfiguration configuration, PostgreSQLDriverProvider driverProvider) {
        super(configuration);
        this.driverProvider = driverProvider;

        initialize();
    }

    @Override
    protected DataSource getConfiguredDataSource(DbBackendConfiguration configuration, String poolName) {
        return driverProvider.getConfiguredDataSource(configuration, poolName);
    }

    @Override
    protected void setGlobalCursorTransactionIsolation(@Nonnull HikariDataSource dataSource) {
        dataSource.setTransactionIsolation("TRANSACTION_REPEATABLE_READ");
    }
}
