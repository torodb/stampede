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

import com.google.inject.Injector;
import com.torodb.core.bundle.BundleConfig;
import com.torodb.core.supervision.Supervisor;

import java.util.concurrent.ThreadFactory;



/**
 * Configuration data used by the backend.
 */
public class BackendConfigImpl implements BackendConfig {

  private final long connectionPoolTimeout;
  private final int connectionPoolSize;
  private final int reservedReadPoolSize;
  private final String username;
  private final String password;
  private final String dbHost;
  private final String dbName;
  private final int dbPort;
  private final boolean includeForeignKeys;
  private final BundleConfig delegate;
  private final boolean sslEnabled;

  protected BackendConfigImpl(long connectionPoolTimeout, int connectionPoolSize,
      int reservedReadPoolSize, String username, String password, String dbHost, String dbName,
      int dbPort, boolean includeForeignKeys, BundleConfig delegate, boolean sslEnabled) {
    this.connectionPoolTimeout = connectionPoolTimeout;
    this.connectionPoolSize = connectionPoolSize;
    this.reservedReadPoolSize = reservedReadPoolSize;
    this.username = username;
    this.password = password;
    this.dbHost = dbHost;
    this.dbName = dbName;
    this.dbPort = dbPort;
    this.includeForeignKeys = includeForeignKeys;
    this.delegate = delegate;
    this.sslEnabled = sslEnabled;
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
  public Injector getEssentialInjector() {
    return delegate.getEssentialInjector();
  }

  @Override
  public ThreadFactory getThreadFactory() {
    return delegate.getThreadFactory();
  }

  @Override
  public Supervisor getSupervisor() {
    return delegate.getSupervisor();
  }

  @Override
  public boolean includeForeignKeys() {
    return includeForeignKeys;
  }
  
  public boolean getSslEnabled() {
    return sslEnabled;
  }
}
