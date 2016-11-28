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

import com.eightkdata.mongowp.bson.BsonValue;
import com.eightkdata.mongowp.server.api.oplog.CollectionOplogOperation;
import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.torodb.mongodb.repl.oplogreplier.ApplierContext;
import com.torodb.mongodb.repl.oplogreplier.analyzed.AnalyzedOp;
import com.torodb.mongodb.repl.oplogreplier.analyzed.AnalyzedOpReducer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class CudAnalyzedOplogBatch extends AnalyzedOplogBatch {

  private final List<OplogOperation> originalBatch;
  private final Table<String, String, Map<BsonValue<?>, AnalyzedOp>> jobs;

  public CudAnalyzedOplogBatch(List<OplogOperation> operations, ApplierContext context,
      AnalyzedOpReducer analyzedOpReducer) {
    this.originalBatch = operations;

    jobs = HashBasedTable.create();

    operations.stream()
        .filter((OplogOperation op) -> {
          switch (op.getType()) {
            case DB_CMD: {
              throw new AssertionError("cmd operations are not expected on "
                  + CudAnalyzedOplogBatch.class.getSimpleName() + " but " + op
                  + " was found");
            }
            case DB:
            case NOOP: {
              return false;
            }
            case DELETE:
            case INSERT:
            case UPDATE:
              return true;
            default: {
              throw new AssertionError("Unexpected oplog operation with type "
                  + op.getType());
            }
          }
        })
        .map(op -> (CollectionOplogOperation) op)
        .forEach(op -> reduceToTableEntry(op, context, analyzedOpReducer));
  }

  public Stream<NamespaceJob> streamNamespaceJobs() {
    return jobs.cellSet().stream().map(cell -> new NamespaceJob(cell.getRowKey(), cell
        .getColumnKey(), cell.getValue().values()));
  }

  public List<OplogOperation> getOriginalBatch() {
    return originalBatch;
  }

  @Override
  public <R, A, T extends Throwable> R accept(AnalyzedOplogBatchVisitor<R, A, T> visitor, A arg)
      throws T {
    return visitor.visit(this, arg);
  }

  private void reduceToTableEntry(CollectionOplogOperation op, ApplierContext context,
      AnalyzedOpReducer analyzedOpReducer) {
    String database = op.getDatabase();
    String collection = op.getCollection();

    Map<BsonValue<?>, AnalyzedOp> byDocAnalyzedOps = jobs.get(database, collection);
    if (byDocAnalyzedOps == null) {
      byDocAnalyzedOps = new HashMap<>();
      jobs.put(database, collection, byDocAnalyzedOps);
    }
    analyzedOpReducer.analyzeAndReduce(byDocAnalyzedOps, op, context);
  }

}
