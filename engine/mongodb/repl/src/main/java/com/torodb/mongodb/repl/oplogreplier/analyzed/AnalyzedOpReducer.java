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

package com.torodb.mongodb.repl.oplogreplier.analyzed;

import com.eightkdata.mongowp.bson.BsonValue;
import com.eightkdata.mongowp.server.api.oplog.CollectionOplogOperation;
import com.google.common.base.Preconditions;
import com.torodb.kvdocument.conversion.mongowp.MongoWpConverter;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.mongodb.repl.oplogreplier.ApplierContext;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 *
 */
public class AnalyzedOpReducer {

  private final boolean onDebug;

  public AnalyzedOpReducer(boolean onDebug) {
    this.onDebug = onDebug;
  }

  public Map<BsonValue<?>, AnalyzedOp> analyzeAndReduce(
      Stream<CollectionOplogOperation> ops, ApplierContext context) {
    HashMap<BsonValue<?>, AnalyzedOp> map = new HashMap<>();

    ops.forEach(op -> {
      analyzeAndReduce(map, op, context);
    });

    return map;
  }

  public void analyzeAndReduce(Map<BsonValue<?>, AnalyzedOp> map,
      CollectionOplogOperation op, ApplierContext context) {
    Preconditions.checkArgument(op.getDocId() != null,
        "Modifications without _id cannot be replicated on parallel");

    AnalyzedOp oldOp = map.get(op.getDocId());
    if (oldOp == null) {
      KvValue<?> translated = MongoWpConverter.translate(op.getDocId());
      if (onDebug) {
        oldOp = new DebuggingAnalyzedOp(translated);
      } else {
        oldOp = new NoopAnalyzedOp(translated);
      }
    }
    AnalyzedOp newAnalyzedOp = oldOp.apply(op, context);
    map.put(op.getDocId(), newAnalyzedOp);
  }
}
