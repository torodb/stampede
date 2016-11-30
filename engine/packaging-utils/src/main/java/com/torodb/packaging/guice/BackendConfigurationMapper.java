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

package com.torodb.packaging.guice;

import com.google.inject.Inject;
import com.torodb.backend.BackendConfiguration;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public abstract class BackendConfigurationMapper implements BackendConfiguration {

  private final long cursorTimeout;
  private final long connectionPoolTimeout;
  private final int connectionPoolSize;
  private final int reservedReadPoolSize;
  private final String dbHost;
  private final int dbPort;
  private final String dbName;
  private final String username;
  private final String password;
  private boolean includeForeignKeys;

  @Inject
  public BackendConfigurationMapper(long cursorTimeout, long connectionPoolTimeout,
      int connectionPoolSize,
      int reservedReadPoolSize, String dbHost, int dbPort, String dbName, String username,
      String password,
      boolean includeForeignKeys) {
    super();
    this.cursorTimeout = cursorTimeout;
    this.connectionPoolTimeout = connectionPoolTimeout;
    this.connectionPoolSize = connectionPoolSize;
    this.reservedReadPoolSize = reservedReadPoolSize;
    this.dbHost = dbHost;
    this.dbPort = dbPort;
    this.dbName = dbName;
    this.username = username;
    this.password = password;
    this.includeForeignKeys = includeForeignKeys;
  }

  @Override
  public long getCursorTimeout() {
    return cursorTimeout;
  }

  @Override
  public long getConnectionPoolTimeout() {
    return connectionPoolTimeout;
  }

  @Override
  public int getConnectionPoolSize() {
    return connectionPoolSize;
  }

  @Override
  public int getReservedReadPoolSize() {
    return reservedReadPoolSize;
  }

  @Override
  public String getUsername() {
    return username;
  }

  @Override
  public String getPassword() {
    return password;
  }

  @Override
  public String getDbHost() {
    return dbHost;
  }

  @Override
  public String getDbName() {
    return dbName;
  }

  @Override
  public int getDbPort() {
    return dbPort;
  }

  @Override
  public boolean includeForeignKeys() {
    return includeForeignKeys;
  }
}
