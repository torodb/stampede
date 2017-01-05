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

public interface BackendConfigBuilder {

  public BackendConfigBuilder setConnectionPoolTimeout(long connectionPoolTimeout);

  public BackendConfigBuilder setConnectionPoolSize(int connectionPoolSize);

  public BackendConfigBuilder setReservedReadPoolSize(int reservedReadPoolSize);

  public BackendConfigBuilder setUsername(String username);

  public BackendConfigBuilder setPassword(String password);

  public BackendConfigBuilder setDbHost(String dbHost);

  public BackendConfigBuilder setDbName(String dbName);

  public BackendConfigBuilder setDbPort(int dbPort);

  public BackendConfigBuilder setIncludeForeignKeys(boolean includeForeignKeys);

  public BackendConfig build();

}
