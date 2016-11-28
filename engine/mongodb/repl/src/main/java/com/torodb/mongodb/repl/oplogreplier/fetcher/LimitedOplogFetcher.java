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

package com.torodb.mongodb.repl.oplogreplier.fetcher;

import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.eightkdata.mongowp.server.api.pojos.MongoCursor;
import com.eightkdata.mongowp.server.api.pojos.MongoCursor.Batch;
import com.eightkdata.mongowp.server.api.pojos.MongoCursor.DeadCursorException;
import com.torodb.mongodb.repl.oplogreplier.FinishedOplogBatch;
import com.torodb.mongodb.repl.oplogreplier.NormalOplogBatch;
import com.torodb.mongodb.repl.oplogreplier.NotReadyForMoreOplogBatch;
import com.torodb.mongodb.repl.oplogreplier.OplogBatch;
import com.torodb.mongodb.repl.oplogreplier.RollbackReplicationException;
import com.torodb.mongodb.repl.oplogreplier.StopReplicationException;

public class LimitedOplogFetcher implements OplogFetcher {

  private final MongoCursor<OplogOperation> cursor;

  public LimitedOplogFetcher(MongoCursor<OplogOperation> cursor) {
    this.cursor = cursor;
  }

  @Override
  public OplogBatch fetch() throws StopReplicationException, RollbackReplicationException {
    //TODO: Add a logic simmilar to ContinuousOplogFetcher to conserve oplog
    //replication semantics!!!
    try {
      if (cursor.isClosed()) {
        return FinishedOplogBatch.getInstance();
      }
      Batch<OplogOperation> batch = cursor.tryFetchBatch();
      if (batch == null || !batch.hasNext()) {
        if (cursor.isTailable()) {
          return NotReadyForMoreOplogBatch.getInstance();
        } else {
          cursor.close();
          return FinishedOplogBatch.getInstance();
        }
      }
      return new NormalOplogBatch(batch.asList(), true);
    } catch (MongoException ex) {
      throw new RollbackReplicationException(ex);
    } catch (DeadCursorException ex) {
      return FinishedOplogBatch.getInstance();
    }
  }

  @Override
  public void close() {
    cursor.close();
  }

}
