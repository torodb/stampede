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

import com.google.inject.Injector;
import com.torodb.backend.BackendConfig;
import com.torodb.core.supervision.Supervisor;

import java.util.concurrent.ThreadFactory;

/**
 * Configuration data for the backend.
 */
public class DerbyDbBackendConfig implements BackendConfig {

  private final boolean inMemory;
  private final boolean embedded;
  private final BackendConfig delegate;

  public DerbyDbBackendConfig(boolean inMemory, boolean embedded, BackendConfig delegate) {
    this.inMemory = inMemory;
    this.embedded = embedded;
    this.delegate = delegate;
  }

  public boolean isInMemory() {
    return inMemory;
  }

  public boolean isEmbedded() {
    return embedded;
  }

  @Override
  public Injector getEssentialInjector() {
    return delegate.getEssentialInjector();
  }

  @Override
  public long getConnectionPoolTimeout() {
    return delegate.getConnectionPoolTimeout();
  }

  @Override
  public int getConnectionPoolSize() {
    return delegate.getConnectionPoolSize();
  }

  @Override
  public int getReservedReadPoolSize() {
    return delegate.getReservedReadPoolSize();
  }

  @Override
  public String getUsername() {
    return delegate.getUsername();
  }

  @Override
  public String getPassword() {
    return delegate.getPassword();
  }

  @Override
  public String getDbHost() {
    return delegate.getDbHost();
  }

  @Override
  public String getDbName() {
    return delegate.getDbName();
  }

  @Override
  public int getDbPort() {
    return delegate.getDbPort();
  }

  @Override
  public boolean includeForeignKeys() {
    return delegate.includeForeignKeys();
  }

  @Override
  public ThreadFactory getThreadFactory() {
    return delegate.getThreadFactory();
  }

  @Override
  public Supervisor getSupervisor() {
    return delegate.getSupervisor();
  }
}
