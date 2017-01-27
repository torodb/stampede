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

package com.torodb.mongodb.repl;

import com.eightkdata.mongowp.client.core.MongoClientFactory;

public class ReplCoreExtInt {

  private final OplogManager oplogManager;
  private final MongoClientFactory mongoClientFactory;
  private final OplogReaderProvider oplogReaderProvider;
  private final ReplMetrics replMetrics;

  public ReplCoreExtInt(OplogManager oplogManager, MongoClientFactory mongoClientFactory,
      OplogReaderProvider oplogReaderProvider, ReplMetrics replMetrics) {
    this.oplogManager = oplogManager;
    this.mongoClientFactory = mongoClientFactory;
    this.oplogReaderProvider = oplogReaderProvider;
    this.replMetrics = replMetrics;
  }

  public OplogManager getOplogManager() {
    return oplogManager;
  }

  public MongoClientFactory getMongoClientFactory() {
    return mongoClientFactory;
  }

  public OplogReaderProvider getOplogReaderProvider() {
    return oplogReaderProvider;
  }

  public ReplMetrics getReplMetrics() {
    return replMetrics;
  }

}
