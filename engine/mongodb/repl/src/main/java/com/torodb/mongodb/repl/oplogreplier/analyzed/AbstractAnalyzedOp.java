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

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.server.api.oplog.InsertOplogOperation;
import com.eightkdata.mongowp.server.api.oplog.UpdateOplogOperation;
import com.torodb.kvdocument.conversion.mongowp.MongoWpConverter;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.kvdocument.values.KvValue;

import java.util.function.Function;

import javax.annotation.Nullable;

/**
 *
 */
public abstract class AbstractAnalyzedOp extends AnalyzedOp {

  private final KvValue<?> mongoDocId;
  private final AnalyzedOpType type;
  @Nullable
  private final Function<KvDocument, KvDocument> calculateFun;

  public AbstractAnalyzedOp(KvValue<?> mongoDocId, AnalyzedOpType type,
      @Nullable Function<KvDocument, KvDocument> calculateFun) {
    this.mongoDocId = mongoDocId;
    this.type = type;
    this.calculateFun = calculateFun;
  }

  abstract AnalyzedOp andThenInsert(KvDocument doc);

  @Override
  AnalyzedOp andThenInsert(InsertOplogOperation op) {
    return andThenInsert(MongoWpConverter.toEagerDocument(op.getDocToInsert()));
  }

  @Override
  public abstract String toString();

  @Override
  AnalyzedOp andThenUpsertSet(UpdateOplogOperation op) {
    return andThenInsert(UpdateActionsTool.applyAsUpsert(op));
  }

  @Override
  public AnalyzedOpType getType() {
    return type;
  }

  @Override
  public KvValue<?> getMongoDocId() {
    return mongoDocId;
  }

  @Override
  public Status<?> getMismatchErrorMessage() throws UnsupportedOperationException {
    if (!requiresMatch()) {
      throw new UnsupportedOperationException();
    }
    return Status.from(ErrorCode.OPERATION_FAILED);
  }

  @Override
  public KvDocument calculateDocToInsert(Function<AnalyzedOp, KvDocument> fetchedDocFun) {
    if (calculateFun == null) {
      return null;
    }
    KvDocument fetchedDoc = null;
    if (requiresFetch()) {
      fetchedDoc = fetchedDocFun.apply(this);
    }
    return calculateFun.apply(fetchedDoc);
  }

  protected final Function<KvDocument, KvDocument> createUpdateSetAsDocument(
      UpdateOplogOperation op) {
    //This is more a warning than an upsertion. Remove if want to do fancy things
    assert UpdateActionsTool.isSetModification(op);

    //A simple assertion to fail before the callback is called when an illegal update is recived
    assert UpdateActionsTool.applyAsUpsert(op) != null;

    return (ignored) -> UpdateActionsTool.applyAsUpsert(op);
  }

  protected final Function<KvDocument, KvDocument> createUpdateMergeChain(UpdateOplogOperation op) {
    //A simple assertion to fail before the callback is called when an illegal update is recived
    assert UpdateActionsTool.parseUpdateAction(op) != null;

    assert calculateFun != null;

    return (fetch) -> UpdateActionsTool.applyModification(
        calculateFun.apply(fetch),
        UpdateActionsTool.parseUpdateAction(op)
    );
  }
}
