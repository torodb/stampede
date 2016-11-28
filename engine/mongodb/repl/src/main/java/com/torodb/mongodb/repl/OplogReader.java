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

import com.eightkdata.mongowp.OpTime;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.exceptions.OplogOperationUnsupported;
import com.eightkdata.mongowp.exceptions.OplogStartMissingException;
import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.eightkdata.mongowp.server.api.pojos.MongoCursor;
import com.google.common.net.HostAndPort;

import java.io.Closeable;

import javax.annotation.Nonnull;

/**
 *
 */
public interface OplogReader extends Closeable {

  public HostAndPort getSyncSource();

  /**
   * Returns a cursor that iterates over the oplog entries of the sync source whose optime is equal
   * or higher than the given one.
   */
  public MongoCursor<OplogOperation> queryGte(OpTime lastFetchedOpTime) throws MongoException;

  /**
   *
   * @return the last operation applied by the sync source
   * @throws OplogStartMissingException if there no operation stored on the sync source
   * @throws OplogOperationUnsupported
   * @throws MongoException
   */
  @Nonnull
  public OplogOperation getLastOp() throws OplogStartMissingException, OplogOperationUnsupported,
      MongoException;

  public OplogOperation getFirstOp() throws OplogStartMissingException, OplogOperationUnsupported,
      MongoException;

  /**
   * Close the reader and all resources associated with him.
   */
  @Override
  public void close();

  public boolean isClosed();

  /**
   * Returns a cursor that iterates throw all oplog operations on the remote oplog whose optime
   * between <em>from</em> and <em>to</em>.
   *
   * @param from
   * @param includeFrom true iff the oplog whose optime is <em>from</em> must be returned
   * @param to
   * @param includeTo   true iff the oplog whose optime is <em>to</em> must be returned
   * @return
   * @throws OplogStartMissingException
   * @throws OplogOperationUnsupported
   * @throws MongoException
   */
  public MongoCursor<OplogOperation> between(OpTime from, boolean includeFrom, OpTime to,
      boolean includeTo)
      throws OplogStartMissingException, OplogOperationUnsupported, MongoException;
}
