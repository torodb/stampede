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

import com.eightkdata.mongowp.client.core.MongoConnection;
import com.eightkdata.mongowp.client.core.UnreachableMongoServerException;
import com.google.common.net.HostAndPort;
import com.torodb.mongodb.repl.exceptions.NoSyncSourceFoundException;

import javax.annotation.Nonnull;

/**
 *
 */
public interface OplogReaderProvider {

  /**
   * Returns new oplog reader.
   *
   * The created reader uses the given host and port as sync source
   *
   * @param syncSource
   * @param mongoClientOptions
   * @param mongoCredential
   * @return
   * @throws NoSyncSourceFoundException
   * @throws UnreachableMongoServerException
   */
  @Nonnull
  public OplogReader newReader(@Nonnull HostAndPort syncSource)
      throws NoSyncSourceFoundException, UnreachableMongoServerException;

  public OplogReader newReader(@Nonnull MongoConnection connection);
}
