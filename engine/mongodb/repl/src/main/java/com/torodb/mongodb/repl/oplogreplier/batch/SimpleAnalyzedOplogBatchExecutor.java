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

import com.codahale.metrics.Timer.Context;
import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.google.common.util.concurrent.AbstractService;
import com.torodb.core.exceptions.user.UniqueIndexViolationException;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.retrier.Retrier;
import com.torodb.core.retrier.Retrier.Hint;
import com.torodb.core.retrier.RetrierAbortException;
import com.torodb.core.retrier.RetrierGiveUpException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.mongodb.core.ExclusiveWriteMongodTransaction;
import com.torodb.mongodb.core.MongodConnection;
import com.torodb.mongodb.core.MongodServer;
import com.torodb.mongodb.core.WriteMongodTransaction;
import com.torodb.mongodb.repl.oplogreplier.ApplierContext;
import com.torodb.mongodb.repl.oplogreplier.OplogOperationApplier;
import com.torodb.mongodb.repl.oplogreplier.OplogOperationApplier.OplogApplyingException;
import com.torodb.mongodb.repl.oplogreplier.batch.AnalyzedOplogBatchExecutor.AnalyzedOplogBatchExecutorMetrics;

import java.util.Iterator;
import java.util.List;

import javax.inject.Inject;

/**
 *
 */
public class SimpleAnalyzedOplogBatchExecutor extends AbstractService
    implements AnalyzedOplogBatchExecutor {

  private final AnalyzedOplogBatchExecutorMetrics metrics;
  private final OplogOperationApplier oplogOperationApplier;
  private final MongodServer server;
  private final Retrier retrier;
  private final NamespaceJobExecutor namespaceJobExecutor;

  @Inject
  public SimpleAnalyzedOplogBatchExecutor(
      AnalyzedOplogBatchExecutorMetrics metrics,
      OplogOperationApplier oplogOperationApplier, MongodServer server,
      Retrier retrier, NamespaceJobExecutor namespaceJobExecutor) {
    this.metrics = metrics;
    this.oplogOperationApplier = oplogOperationApplier;
    this.server = server;
    this.retrier = retrier;
    this.namespaceJobExecutor = namespaceJobExecutor;
  }

  @Override
  protected void doStart() {
    notifyStarted();
  }

  @Override
  protected void doStop() {
    notifyStopped();
  }

  @Override
  public void execute(OplogOperation op, ApplierContext context)
      throws OplogApplyingException, RollbackException, UserException {
    try (MongodConnection connection = server.openConnection();
        ExclusiveWriteMongodTransaction mongoTransaction = connection
            .openExclusiveWriteTransaction()) {

      oplogOperationApplier.apply(op, mongoTransaction, context);
      mongoTransaction.commit();
    }
  }

  @Override
  public void execute(CudAnalyzedOplogBatch cudBatch, ApplierContext context)
      throws RollbackException, UserException, NamespaceJobExecutionException {
    try (MongodConnection connection = server.openConnection()) {

      Iterator<NamespaceJob> it = cudBatch.streamNamespaceJobs().iterator();
      while (it.hasNext()) {
        execute(it.next(), context, connection);
      }
    }
  }

  protected void execute(NamespaceJob job, ApplierContext applierContext,
      MongodConnection connection) throws RollbackException, UserException,
      NamespaceJobExecutionException {
    try (Context timerContext = metrics.getNamespaceBatchTimer().time()) {
      boolean optimisticDeleteAndCreate = applierContext.isReapplying().orElse(true);
      try {
        execute(job, applierContext, connection, optimisticDeleteAndCreate);
      } catch (UniqueIndexViolationException ex) {
        assert optimisticDeleteAndCreate : "Unique index violations should not happen when "
            + "pesimistic delete and create is executed";
        execute(job, applierContext, connection, false);
      }
    }
  }

  private void execute(NamespaceJob job, ApplierContext applierContext,
      MongodConnection connection, boolean optimisticDeleteAndCreate)
      throws RollbackException, UserException, NamespaceJobExecutionException,
      UniqueIndexViolationException {
    try (WriteMongodTransaction mongoTransaction = connection.openWriteTransaction()) {
      namespaceJobExecutor.apply(job, mongoTransaction, applierContext, optimisticDeleteAndCreate);
      mongoTransaction.commit();
    }
  }

  @Override
  public OplogOperation visit(SingleOpAnalyzedOplogBatch batch, ApplierContext arg) throws
      RetrierGiveUpException {
    OplogOperation operation = batch.getOperation();

    try (Context context = metrics.getSingleOpTimer(operation).time()) {
      try {
        execute(batch.getOperation(), arg);
      } catch (OplogApplyingException | UserException ex) {
        throw new RetrierGiveUpException("Unexpected exception while replying", ex);
      } catch (RollbackException ex) {
        ApplierContext retryingReplingContext = new ApplierContext.Builder()
            .setReapplying(true)
            .setUpdatesAsUpserts(true)
            .build();
        retrier.retry(() -> {
          try {
            execute(batch.getOperation(), retryingReplingContext);
            return Empty.getInstance();
          } catch (OplogApplyingException ex2) {
            throw new RetrierAbortException("Unexpected exception while replying", ex2);
          }
        }, Hint.CRITICAL, Hint.TIME_SENSIBLE);
      }
    }

    return batch.getOperation();
  }

  @Override
  public OplogOperation visit(CudAnalyzedOplogBatch batch, ApplierContext arg) throws
      RetrierGiveUpException {
    metrics.getCudBatchSize().update(batch.getOriginalBatch().size());
    try (Context context = metrics.getCudBatchTimer().time()) {
      try {
        execute(batch, arg);
      } catch (UserException | NamespaceJobExecutionException ex) {
        throw new RetrierGiveUpException("Unexpected exception while replying", ex);
      } catch (RollbackException ex) {
        ApplierContext retryingReplingContext = new ApplierContext.Builder()
            .setReapplying(true)
            .setUpdatesAsUpserts(true)
            .build();
        retrier.retry(() -> {
          try {
            execute(batch, retryingReplingContext);
            return Empty.getInstance();
          } catch (UserException | NamespaceJobExecutionException ex2) {
            throw new RetrierAbortException("Unexpected user exception while applying "
                + "the batch " + batch, ex2);
          }
        }, Hint.CRITICAL, Hint.TIME_SENSIBLE);
      }
    }

    List<OplogOperation> originalBatch = batch.getOriginalBatch();
    return originalBatch.get(originalBatch.size() - 1);
  }

  protected MongodServer getServer() {
    return server;
  }
}
