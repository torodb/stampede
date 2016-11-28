/*
 * ToroDB
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.backend.postgresql;

import com.torodb.backend.AbstractDbBackendService;
import com.torodb.backend.TransactionIsolationLevel;
import com.torodb.backend.driver.postgresql.PostgreSqlBackendConfiguration;
import com.torodb.backend.driver.postgresql.PostgreSqlDriverProvider;
import com.torodb.core.annotations.TorodbIdleService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ThreadFactory;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.sql.DataSource;

/**
 * PostgreSQL-based backend.
 */
public class PostgreSqlDbBackend extends AbstractDbBackendService<PostgreSqlBackendConfiguration> {

  private static final Logger LOGGER = LogManager.getLogger(PostgreSqlDbBackend.class);

  private final PostgreSqlDriverProvider driverProvider;

  @Inject
  public PostgreSqlDbBackend(@TorodbIdleService ThreadFactory threadFactory,
      PostgreSqlBackendConfiguration configuration,
      PostgreSqlDriverProvider driverProvider,
      PostgreSqlErrorHandler errorHandler) {
    super(threadFactory, configuration, errorHandler);

    LOGGER.info("Configured PostgreSQL backend at {}:{}", configuration.getDbHost(), configuration
        .getDbPort());

    this.driverProvider = driverProvider;
  }

  @Override
  protected DataSource getConfiguredDataSource(PostgreSqlBackendConfiguration configuration,
      String poolName) {
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
