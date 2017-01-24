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

package com.torodb.mongodb.repl.oplogreplier;

import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonValue;
import com.eightkdata.mongowp.server.api.oplog.DbCmdOplogOperation;
import com.eightkdata.mongowp.server.api.oplog.DbOplogOperation;
import com.eightkdata.mongowp.server.api.oplog.DeleteOplogOperation;
import com.eightkdata.mongowp.server.api.oplog.InsertOplogOperation;
import com.eightkdata.mongowp.server.api.oplog.NoopOplogOperation;
import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.eightkdata.mongowp.server.api.oplog.OplogOperationVisitor;
import com.eightkdata.mongowp.server.api.oplog.UpdateOplogOperation;
import com.torodb.mongodb.utils.DefaultIdUtils;
import com.torodb.mongodb.utils.NamespaceUtil;

/**
 * A utility class that controls that all executed {@link OplogOperation oplog operations} use a
 * scalar _id as filter.
 *
 * <p/>ToroDB does only support queries where the operator is the equality and the value is a
 * scalar, so oplog operations that filter by a complex _id are not supported.
 */
public class ComplexIdOperationFilter {

  private static final FilterVisitor VISITOR = new FilterVisitor();

  private ComplexIdOperationFilter() {}

  public static OplogOperation filter(OplogOperation oplogOp)
      throws UnexpectedOplogOperationException {
    return oplogOp.accept(VISITOR, null);
  }

  public static OplogBatch filter(OplogBatch oplogBatch) throws UnexpectedOplogOperationException {
    oplogBatch.getOps().stream()
        .forEach(ComplexIdOperationFilter::filter);
    return oplogBatch;
  }

  private static class FilterVisitor implements OplogOperationVisitor<OplogOperation, Void> {

    @Override
    public OplogOperation visit(DbCmdOplogOperation op, Void arg) {
      //Nothing to do here
      return op;
    }

    @Override
    public OplogOperation visit(DbOplogOperation op, Void arg) {
      //Nothing to do here
      return op;
    }

    @Override
    public OplogOperation visit(NoopOplogOperation op, Void arg) {
      //Nothing to do here
      return op;
    }

    @Override
    public OplogOperation visit(DeleteOplogOperation op, Void arg) {
      checkFilter(op, op.getFilter(), "filter", NamespaceUtil.isSystem(op.getCollection()));
      return op;
    }

    @Override
    public OplogOperation visit(InsertOplogOperation op, Void arg) {
      checkFilter(op, op.getDocToInsert(), "document to insert",
          NamespaceUtil.isSystem(op.getCollection()));
      return op;
    }

    @Override
    public OplogOperation visit(UpdateOplogOperation op, Void arg) {
      checkFilter(op, op.getFilter(), "filter", NamespaceUtil.isSystem(op.getCollection()));
      return op;
    }

    private void checkFilter(OplogOperation op, BsonDocument filter, String desc, 
        boolean onSystemCol) throws UnexpectedOplogOperationException {
      BsonValue<?> idValue = filter.get(DefaultIdUtils.DEFAULT_ID_KEY);

      if (idValue == null) {
        if (!onSystemCol) {
          throw new AssertionError("OplogOperation " + op + " does not contain a value _id on its "
              + desc);
        }
      } else {
        if (idValue.isArray() || idValue.isDocument()) {
          throw new UnexpectedOplogOperationException(op, "Currently ToroDB does only support "
              + "documents whose _id is a scalar value but the " + desc + " contains the "
              + "non-scalar value " + idValue);
        }
      }
    }

  }
}
