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

package com.torodb.mongodb.repl.impl;

import com.eightkdata.mongowp.client.core.MongoConnection;
import com.google.common.net.HostAndPort;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class ReusedConnectionOplogReader extends AbstractMongoOplogReader {

  private boolean closed;
  /**
   * Not owned, meaning that is not closed when this reader is closed
   */
  private final MongoConnection connection;

  /**
   *
   * @param connection The connection that will be used. It won't be closed when calling
   *                   {@linkplain #close()}
   */
  ReusedConnectionOplogReader(@Nonnull MongoConnection connection) {
    this.connection = connection;
    this.closed = false;
  }

  @Override
  public HostAndPort getSyncSource() {
    return connection.getClientOwner().getAddress();
  }

  @Override
  protected MongoConnection consumeConnection() {
    return connection;
  }

  @Override
  protected void releaseConnection(MongoConnection connection) {
  }

  @Override
  public void close() {
    closed = true;
  }

  @Override
  public boolean isClosed() {
    return closed;
  }
}
