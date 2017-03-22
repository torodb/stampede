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

package com.torodb.backend.derby.driver;

import com.torodb.backend.BackendConfigBuilder;
import com.torodb.backend.BackendConfigImplBuilder;
import com.torodb.core.bundle.BundleConfig;

public class DerbyDbBackendConfigBuilder implements BackendConfigBuilder {
  private boolean inMemory = true;
  private boolean embedded = true;
  private final BackendConfigBuilder delegate;

  public DerbyDbBackendConfigBuilder(BundleConfig genericConfig) {
    this.delegate = new BackendConfigImplBuilder(genericConfig);
    delegate.setDbPort(1527);
  }

  public DerbyDbBackendConfigBuilder(BackendConfigBuilder delegate) {
    this.delegate = delegate;
  }

  public DerbyDbBackendConfigBuilder setInMemory(boolean inMemory) {
    this.inMemory = inMemory;
    return this;
  }

  public DerbyDbBackendConfigBuilder setEmbedded(boolean embedded) {
    this.embedded = embedded;
    return this;
  }

  @Override
  public DerbyDbBackendConfigBuilder setConnectionPoolTimeout(long connectionPoolTimeout) {
    delegate.setConnectionPoolTimeout(connectionPoolTimeout);
    return this;
  }

  @Override
  public DerbyDbBackendConfigBuilder setConnectionPoolSize(int connectionPoolSize) {
    delegate.setConnectionPoolSize(connectionPoolSize);
    return this;
  }

  @Override
  public DerbyDbBackendConfigBuilder setReservedReadPoolSize(int reservedReadPoolSize) {
    delegate.setReservedReadPoolSize(reservedReadPoolSize);
    return this;
  }

  @Override
  public DerbyDbBackendConfigBuilder setUsername(String username) {
    delegate.setUsername(username);
    return this;
  }

  @Override
  public DerbyDbBackendConfigBuilder setPassword(String password) {
    delegate.setPassword(password);
    return this;
  }

  @Override
  public DerbyDbBackendConfigBuilder setDbHost(String dbHost) {
    delegate.setDbHost(dbHost);
    return this;
  }

  @Override
  public DerbyDbBackendConfigBuilder setDbName(String dbName) {
    delegate.setDbName(dbName);
    return this;
  }

  @Override
  public DerbyDbBackendConfigBuilder setDbPort(int dbPort) {
    delegate.setDbPort(dbPort);
    return this;
  }

  @Override
  public DerbyDbBackendConfigBuilder setIncludeForeignKeys(boolean includeForeignKeys) {
    delegate.setIncludeForeignKeys(includeForeignKeys);
    return this;
  }
  
  @Override
  public DerbyDbBackendConfig build() {
    return new DerbyDbBackendConfig(inMemory, embedded, delegate.build());
  }
}
