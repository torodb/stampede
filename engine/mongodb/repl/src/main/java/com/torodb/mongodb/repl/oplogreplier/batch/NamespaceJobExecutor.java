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

import com.eightkdata.mongowp.Status;
import com.torodb.core.cursors.Cursor;
import com.torodb.core.cursors.IteratorCursor;
import com.torodb.core.document.ToroDocument;
import com.torodb.core.exceptions.user.UniqueIndexViolationException;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.transaction.RollbackException;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.mongodb.core.WriteMongodTransaction;
import com.torodb.mongodb.repl.oplogreplier.ApplierContext;
import com.torodb.mongodb.repl.oplogreplier.analyzed.AnalyzedOp;
import com.torodb.mongodb.repl.oplogreplier.analyzed.AnalyzedOpType;
import org.jooq.lambda.tuple.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.concurrent.ThreadSafe;

/**
 *
 */
@ThreadSafe
public class NamespaceJobExecutor {

  private static final AttributeReference _ID_ATT_REF = new AttributeReference.Builder()
      .addObjectKey("_id")
      .build();

  public void apply(NamespaceJob job, WriteMongodTransaction transaction,
      ApplierContext applierContext, boolean optimisticDeleteAndCreate)
      throws RollbackException, UserException, NamespaceJobExecutionException,
      UniqueIndexViolationException {

    Map<AnalyzedOp, Integer> fetchDids = fetchDids(job, transaction, optimisticDeleteAndCreate);

    List<Status<?>> errors = findErrors(job, fetchDids);
    if (!errors.isEmpty()) {
      throw new NamespaceJobExecutionException(job, errors);
    }
    if (errors.isEmpty()) {
      Map<AnalyzedOp, ToroDocument> fetchDocs = fetchDocs(job, transaction, fetchDids);
      deleteDocs(job, transaction, fetchDids);
      insertDocs(job, transaction, fetchDocs);
    }

  }

  /**
   * Returns a map whose entries are the did of each analyzed op that requires to fetch them.
   *
   * @param job
   * @param transaction
   * @return
   * @see AnalyzedOp#requiresToFetchToroId()
   */
  private static Map<AnalyzedOp, Integer> fetchDids(NamespaceJob job,
      WriteMongodTransaction transaction, boolean optimisticDeleteAndCreate) {

    Stream<AnalyzedOp> filteredJobs = job.getJobs().stream()
        .filter(AnalyzedOp::requiresToFetchToroId);

    if (optimisticDeleteAndCreate) {
      filteredJobs = filteredJobs.filter(op -> op.getType() != AnalyzedOpType.DELETE_CREATE);
    }

    Map<KvValue<?>, AnalyzedOp> mapToFetch = filteredJobs
        .collect(Collectors.toMap(
            op -> op.getMongoDocId(),
            Function.identity()
        ));

    return transaction.getTorodTransaction()
        .findByAttRefInProjection(
            job.getDatabase(),
            job.getCollection(),
            _ID_ATT_REF,
            mapToFetch.keySet())
        .getRemaining()
        .stream()
        .collect(Collectors.toMap(
            tuple -> mapToFetch.get(tuple.v2),
            Tuple2::v1)
        );
  }

  /**
   * Returns a list of all mismatching errors on the given job.
   *
   * @param job
   * @param fetchDids
   * @return
   */
  private List<Status<?>> findErrors(NamespaceJob job, Map<AnalyzedOp, Integer> fetchDids) {
    return job.getJobs().stream()
        .filter(AnalyzedOp::requiresMatch) //only care about ops that requires a match
        .filter(op -> !fetchDids.containsKey(op)) //only care about ops that did not match
        .map(AnalyzedOp::getMismatchErrorMessage)
        .collect(Collectors.toList());
  }

  private Map<AnalyzedOp, ToroDocument> fetchDocs(NamespaceJob job,
      WriteMongodTransaction transaction, Map<AnalyzedOp, Integer> fetchDids) {
    Map<Integer, AnalyzedOp> didToOps = job.getJobs().stream()
        .filter(AnalyzedOp::requiresFetch) //only care about ops that requires a fetch
        .collect(Collectors.toMap(
            op -> fetchDids.get(op),
            Function.identity())
        );

    Cursor<Integer> didCursor = new IteratorCursor<>(didToOps.keySet().iterator());
    return transaction.getTorodTransaction()
        .fetch(job.getDatabase(), job.getCollection(), didCursor)
        .asDocCursor()
        .getRemaining()
        .stream()
        .collect(Collectors.toMap(
            toroDoc -> didToOps.get(toroDoc.getId()),
            Function.identity())
        );
  }

  private void deleteDocs(NamespaceJob job, WriteMongodTransaction transaction,
      Map<AnalyzedOp, Integer> fetchDids) {
    if (fetchDids.isEmpty()) {
      return;
    }

    Stream<Integer> didsToDelete = job.getJobs().stream()
        .filter(AnalyzedOp::deletes)
        .map(op -> fetchDids.get(op))
        .filter(did -> did != null);

    transaction.getTorodTransaction().delete(job.getDatabase(), job.getCollection(),
        new IteratorCursor<>(didsToDelete.iterator()));
  }

  private void insertDocs(NamespaceJob job, WriteMongodTransaction transaction,
      Map<AnalyzedOp, ToroDocument> fetchDocs) throws UserException {
    Function<AnalyzedOp, KvDocument> getFetchDocFun = op -> {
      ToroDocument fetchToroDoc = fetchDocs.get(op);
      if (fetchToroDoc == null) {
        return null;
      } else {
        return fetchToroDoc.getRoot();
      }
    };
    Stream<KvDocument> docsToInsert = job.getJobs().stream()
        .map(op -> op.calculateDocToInsert(getFetchDocFun))
        .filter(doc -> doc != null);

    transaction.getTorodTransaction().insert(job.getDatabase(), job.getCollection(), docsToInsert);
  }
}
