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

package com.torodb.engine.mongodb.sharding.isolation;

import com.torodb.torod.TorodConnection;
import com.torodb.torod.TorodServer;

public abstract class ConnDecorator<S extends TorodServer> implements TorodConnection {

  private final S server;
  private final TorodConnection decorated;

  public ConnDecorator(S server, TorodConnection decorate) {
    this.server = server;
    this.decorated = decorate;
  }

  protected TorodConnection getDecorated() {
    return decorated;
  }

  @Override
  public S getServer() {
    return server;
  }

  @Override
  public int getConnectionId() {
    return decorated.getConnectionId();
  }

  @Override
  public boolean isClosed() {
    return decorated.isClosed();
  }

  @Override
  public void close() {
    decorated.close();
  }

}
