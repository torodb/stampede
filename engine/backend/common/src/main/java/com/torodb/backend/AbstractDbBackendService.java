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

package com.torodb.backend;

import com.google.common.base.Preconditions;
import com.torodb.backend.ErrorHandler.Context;
import com.torodb.core.annotations.TorodbIdleService;
import com.torodb.core.services.IdleTorodbService;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ThreadFactory;

import javax.annotation.Nonnull;
import javax.sql.DataSource;

/**
 *
 */
public abstract class AbstractDbBackendService<ConfigurationT extends BackendConfiguration>
    extends IdleTorodbService implements DbBackendService {

  private static final Logger LOGGER = LogManager.getLogger(AbstractDbBackendService.class);

  public static final int SYSTEM_DATABASE_CONNECTIONS = 1;
  public static final int MIN_READ_CONNECTIONS_DATABASE = 1;
  public static final int MIN_SESSION_CONNECTIONS_DATABASE = 2;
  public static final int MIN_CONNECTIONS_DATABASE = SYSTEM_DATABASE_CONNECTIONS
      + MIN_READ_CONNECTIONS_DATABASE
      + MIN_SESSION_CONNECTIONS_DATABASE;

  private final ConfigurationT configuration;
  private final ErrorHandler errorHandler;
  private HikariDataSource writeDataSource;
  private HikariDataSource systemDataSource;
  private HikariDataSource readOnlyDataSource;
  /**
   * Global state variable for data import mode. If true data import mode is enabled, data import
   * mode is otherwise disabled. Indexes will not be created while data import mode is enabled. When
   * this mode is enabled importing data will be faster.
   */
  private volatile boolean dataImportMode;

  /**
   * Configure the backend. The contract specifies that any subclass must call initialize() method
   * after properly constructing the object.
   *
   * @param threadFactory the thread factory that will be used to create the startup and shutdown
   *                      threads
   * @param configuration
   * @param errorHandler
   */
  public AbstractDbBackendService(@TorodbIdleService ThreadFactory threadFactory,
      ConfigurationT configuration, ErrorHandler errorHandler) {
    super(threadFactory);
    this.configuration = configuration;
    this.errorHandler = errorHandler;
    this.dataImportMode = false;

    int connectionPoolSize = configuration.getConnectionPoolSize();
    int reservedReadPoolSize = configuration.getReservedReadPoolSize();
    Preconditions.checkState(
        connectionPoolSize >= MIN_CONNECTIONS_DATABASE,
        "At least " + MIN_CONNECTIONS_DATABASE
        + " total connections with the backend SQL database are required"
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
      justification =
      "Object lifecyle is managed as a Service. Datasources are initialized in setup method")
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
      ConfigurationT configuration, String poolName, int poolSize,
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
     * TODO: implement to add metric support. See
     * https://github.com/brettwooldridge/HikariCP/wiki/Codahale-Metrics
     * hikariConfig.setMetricRegistry(...);
     */

    LOGGER.info("Created pool {} with size {} and level {}", poolName, poolSize,
        transactionIsolationLevel.name());

    return new HikariDataSource(hikariConfig);
  }

  protected abstract DataSource getConfiguredDataSource(ConfigurationT configuration,
      String poolName);

  @Override
  public void disableDataInsertMode() {
    this.dataImportMode = false;
  }

  @Override
  public void enableDataInsertMode() {
    this.dataImportMode = true;
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
      throw new IllegalStateException("The " + serviceName() + " is not running");
    }
  }

  @Override
  public long getDefaultCursorTimeout() {
    return configuration.getCursorTimeout();
  }

  @Override
  public boolean isOnDataInsertMode() {
    return dataImportMode;
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
