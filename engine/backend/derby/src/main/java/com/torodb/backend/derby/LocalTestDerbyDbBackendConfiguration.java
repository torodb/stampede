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

package com.torodb.backend.derby;

import com.torodb.backend.driver.derby.DerbyDbBackendConfiguration;

public class LocalTestDerbyDbBackendConfiguration implements DerbyDbBackendConfiguration {

  @Override
  public boolean inMemory() {
    return true;
  }

  @Override
  public boolean embedded() {
    return true;
  }

  @Override
  public long getCursorTimeout() {
    return 10L * 60 * 1000;
  }

  @Override
  public long getConnectionPoolTimeout() {
    return 10_000;
  }

  @Override
  public int getConnectionPoolSize() {
    return 30;
  }

  @Override
  public int getReservedReadPoolSize() {
    return 10;
  }

  @Override
  public String getUsername() {
    return "torodb";
  }

  @Override
  public String getPassword() {
    return null;
  }

  @Override
  public String getDbHost() {
    return "localhost";
  }

  @Override
  public String getDbName() {
    return "torod";
  }

  @Override
  public int getDbPort() {
    return 1527;
  }

  @Override
  public boolean includeForeignKeys() {
    return false;
  }

}
