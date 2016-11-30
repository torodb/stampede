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

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.server.api.oplog.CollectionOplogOperation;
import com.eightkdata.mongowp.server.api.oplog.DeleteOplogOperation;
import com.eightkdata.mongowp.server.api.oplog.InsertOplogOperation;
import com.eightkdata.mongowp.server.api.oplog.UpdateOplogOperation;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.mongodb.repl.oplogreplier.ApplierContext;

import java.util.function.Function;

import javax.annotation.Nullable;

/**
 *
 */
public abstract class AnalyzedOp {

  public abstract KvValue<?> getMongoDocId();

  public abstract AnalyzedOpType getType();

  /**
   * Returns the document that have be inserted when this operation is executed.
   *
   * @param fetchedDocFun
   * @return The document that have be inserted or null if no document have to be inserted
   */
  @Nullable
  public abstract KvDocument calculateDocToInsert(
      @Nullable Function<AnalyzedOp, KvDocument> fetchedDocFun);

  final AnalyzedOp apply(CollectionOplogOperation colOp, ApplierContext context) {
    switch (colOp.getType()) {
      case INSERT:
        return andThenInsert((InsertOplogOperation) colOp);
      case DELETE:
        return andThenDelete((DeleteOplogOperation) colOp);
      case UPDATE:
        UpdateOplogOperation op = (UpdateOplogOperation) colOp;
        if (context.treatUpdateAsUpsert() || op.isUpsert()) {
          if (UpdateActionsTool.isSetModification(op)) {
            //UpsertSet is always like an insert
            return andThenUpsertSet(op);
          } else {
            return andThenUpsertMod(op);
          }
        } else {
          if (UpdateActionsTool.isSetModification(op)) {
            return andThenUpdateSet(op);
          } else {
            return andThenUpdateMod(op);
          }
        }
      default:
        throw new AssertionError("Unexpected oplog operation " + colOp.getType());
    }
  }

  abstract AnalyzedOp andThenInsert(InsertOplogOperation op);

  abstract AnalyzedOp andThenUpdateMod(UpdateOplogOperation op);

  abstract AnalyzedOp andThenUpdateSet(UpdateOplogOperation op);

  abstract AnalyzedOp andThenUpsertMod(UpdateOplogOperation op);

  abstract AnalyzedOp andThenUpsertSet(UpdateOplogOperation op);

  abstract AnalyzedOp andThenDelete(DeleteOplogOperation op);

  public final boolean requiresToFetchToroId() {
    return getType().requiresToFetchToroId();
  }

  public final boolean requiresMatch() {
    return getType().requiresMatch();
  }

  public final boolean requiresFetch() {
    return getType().requiresFetch();
  }

  public final boolean deletes() {
    return getType().deletes();
  }

  /**
   * Returns the error status that should be shown to the user if the operation fail because it
   * expected a match on the database, but the modified document was not there.
   *
   * It is important to know that not all operations can fail because of this. If the operation
   * cannot fail because of this, {@link UnsupportedOperationException} is thrown.
   *
   * @return
   * @throws UnsupportedOperationException if
   *                                       {@link #getType()}#{@link AnalyzedOpType#requiresMatch()}
   *                                       return false
   */
  public abstract Status<?> getMismatchErrorMessage() throws UnsupportedOperationException;

}
