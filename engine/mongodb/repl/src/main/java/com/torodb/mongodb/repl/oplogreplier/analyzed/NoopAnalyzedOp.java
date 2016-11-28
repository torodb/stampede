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
import com.torodb.kvdocument.values.heap.MapKvDocument;
import com.torodb.mongodb.language.update.UpdateAction;

import java.util.LinkedHashMap;
import java.util.function.Function;

/**
 *
 */
public class NoopAnalyzedOp extends AbstractAnalyzedOp {

  NoopAnalyzedOp(KvValue<?> mongoDocId) {
    super(mongoDocId, AnalyzedOpType.NOOP, Function.identity());
  }

  @Override
  public AnalyzedOp andThenInsert(KvDocument doc) {
    return new DeleteCreateAnalyzedOp(getMongoDocId(), doc);
  }

  @Override
  public AnalyzedOp andThenUpdateMod(UpdateOplogOperation op) {
    //A simple assertion to fail before the callback is called when an illegal update is recived
    assert UpdateActionsTool.parseUpdateAction(op) != null;

    Function<KvDocument, KvDocument> calculateFun = (fetched) -> {
      UpdateAction updateAction = UpdateActionsTool.parseUpdateAction(op);
      return UpdateActionsTool.applyModification(fetched, updateAction);
    };

    return new UpdateModAnalyzedOp(getMongoDocId(), calculateFun);
  }

  @Override
  public AnalyzedOp andThenUpdateSet(UpdateOplogOperation op) {
    return new UpdateSetAnalyzedOp(getMongoDocId(), createUpdateSetAsDocument(op));
  }

  @Override
  public AnalyzedOp andThenUpsertMod(UpdateOplogOperation op) {
    //A simple assertion to fail before the callback is called when an illegal update is recived
    assert UpdateActionsTool.parseUpdateAction(op) != null;

    Function<KvDocument, KvDocument> calculateFun = (fetched) -> {
      KvDocument newFetched = fetched;
      if (newFetched == null) {
        LinkedHashMap<String, KvValue<?>> map = new LinkedHashMap<>(1);
        map.put("_id", getMongoDocId());
        newFetched = new MapKvDocument(map);
      }
      UpdateAction updateAction = UpdateActionsTool.parseUpdateAction(op);
      return UpdateActionsTool.applyModification(newFetched, updateAction);
    };

    return new UpsertModAnalyzedOp(getMongoDocId(), calculateFun);
  }

  @Override
  public AnalyzedOp andThenDelete(DeleteOplogOperation op) {
    return new DeleteAnalyzedOp(getMongoDocId());
  }

  @Override
  public String toString() {
    return "noop(" + getMongoDocId() + ')';
  }
}
