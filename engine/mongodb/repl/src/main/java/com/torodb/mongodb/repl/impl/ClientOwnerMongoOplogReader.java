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

import com.eightkdata.mongowp.client.core.MongoClient;
import com.eightkdata.mongowp.client.core.MongoConnection;
import com.google.common.net.HostAndPort;

/**
 *
 */
public class ClientOwnerMongoOplogReader extends AbstractMongoOplogReader {

  private final MongoClient mongoClient;

  public ClientOwnerMongoOplogReader(MongoClient mongoClient) {
    this.mongoClient = mongoClient;
  }

  @Override
  public HostAndPort getSyncSource() {
    return mongoClient.getAddress();
  }

  @Override
  protected MongoConnection consumeConnection() {
    return mongoClient.openConnection();
  }

  @Override
  protected void releaseConnection(MongoConnection connection) {
    connection.close();
  }

  @Override
  public void close() {
    mongoClient.close();
  }

  @Override
  public boolean isClosed() {
    return mongoClient.isClosed();
  }

}
