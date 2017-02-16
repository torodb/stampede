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

package com.torodb.mongodb.repl.oplogreplier.batch;

import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.torodb.mongodb.repl.oplogreplier.UnexpectedOplogOperationException;

import javax.inject.Inject;

/**
 * A class that checks if all operations on the given {@link OplogBatch} are supported.
 */
@SuppressWarnings("serial")
public class OplogBatchChecker implements akka.japi.function.Function<OplogBatch, OplogBatch> {
  private final OplogOperationChecker opChecker;

  @Inject
  public OplogBatchChecker(OplogOperationChecker oplogOperationChecker) {
    this.opChecker = oplogOperationChecker;
  }

  public void check(OplogBatch oplogBatch) {
    for (OplogOperation op : oplogBatch.getOps()) {
      opChecker.check(op);
    }
  }

  @Override
  public OplogBatch apply(OplogBatch batch) throws Exception {
    check(batch);
    return batch;
  }

  public static interface OplogOperationChecker {

    public void check(OplogOperation op) throws UnexpectedOplogOperationException;

  }
}
