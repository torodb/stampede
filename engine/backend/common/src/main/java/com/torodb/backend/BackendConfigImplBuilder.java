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

import com.torodb.core.modules.BundleConfig;

public class BackendConfigImplBuilder implements BackendConfigBuilder {

  private long connectionPoolTimeout = 10_000L;
  private int connectionPoolSize = 100;
  private int reservedReadPoolSize = 20;
  private String username = "torodb";
  private String password;
  private String dbHost = "localhost";
  private String dbName = "torodb";
  private int dbPort;
  private boolean includeForeignKeys = true;
  private final BundleConfig generalConfig;
  private boolean sslEnabled = false;

  public BackendConfigImplBuilder(BundleConfig generalConfig) {
    this.generalConfig = generalConfig;
  }

  @Override
  public BackendConfigImplBuilder setConnectionPoolTimeout(long connectionPoolTimeout) {
    this.connectionPoolTimeout = connectionPoolTimeout;
    return this;
  }

  @Override
  public BackendConfigImplBuilder setConnectionPoolSize(int connectionPoolSize) {
    this.connectionPoolSize = connectionPoolSize;
    return this;
  }

  @Override
  public BackendConfigImplBuilder setReservedReadPoolSize(int reservedReadPoolSize) {
    this.reservedReadPoolSize = reservedReadPoolSize;
    return this;
  }

  @Override
  public BackendConfigImplBuilder setUsername(String username) {
    this.username = username;
    return this;
  }

  @Override
  public BackendConfigImplBuilder setPassword(String password) {
    this.password = password;
    return this;
  }

  @Override
  public BackendConfigImplBuilder setDbHost(String dbHost) {
    this.dbHost = dbHost;
    return this;
  }

  @Override
  public BackendConfigImplBuilder setDbName(String dbName) {
    this.dbName = dbName;
    return this;
  }

  @Override
  public BackendConfigImplBuilder setDbPort(int dbPort) {
    this.dbPort = dbPort;
    return this;
  }

  @Override
  public BackendConfigImplBuilder setIncludeForeignKeys(boolean includeForeignKeys) {
    this.includeForeignKeys = includeForeignKeys;
    return this;
  }
  
  public BackendConfigImplBuilder setSslEnabled(boolean sslEnabled) {
    this.sslEnabled = sslEnabled;
    return this;
  }

  @Override
  public BackendConfig build() {
    return new BackendConfigImpl(connectionPoolTimeout, connectionPoolSize, reservedReadPoolSize,
        username, password, dbHost, dbName, dbPort, includeForeignKeys, generalConfig, sslEnabled);
  }

}
