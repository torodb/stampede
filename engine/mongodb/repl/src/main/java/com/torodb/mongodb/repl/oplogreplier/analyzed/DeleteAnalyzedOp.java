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

import com.eightkdata.mongowp.server.api.oplog.DeleteOplogOperation;
import com.eightkdata.mongowp.server.api.oplog.UpdateOplogOperation;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.kvdocument.values.KvValue;

/**
 *
 */
public class DeleteAnalyzedOp extends AbstractAnalyzedOp {

  DeleteAnalyzedOp(KvValue<?> mongoDocId) {
    super(mongoDocId, AnalyzedOpType.DELETE, null);
  }

  @Override
  public AnalyzedOp andThenInsert(KvDocument doc) {
    return new DeleteCreateAnalyzedOp(getMongoDocId(), doc);
  }

  @Override
  public AnalyzedOp andThenUpdateMod(UpdateOplogOperation op) {
    return new ErrorAnalyzedOp(getMongoDocId());
  }

  @Override
  public AnalyzedOp andThenUpdateSet(UpdateOplogOperation op) {
    return new ErrorAnalyzedOp(getMongoDocId());
  }

  @Override
  public AnalyzedOp andThenUpsertMod(UpdateOplogOperation op) {
    return new DeleteCreateAnalyzedOp(getMongoDocId(), createUpdateSetAsDocument(op));
  }

  @Override
  public AnalyzedOp andThenDelete(DeleteOplogOperation op) {
    return this;
  }

  @Override
  public String toString() {
    return "d(" + getMongoDocId() + ')';
  }
}
