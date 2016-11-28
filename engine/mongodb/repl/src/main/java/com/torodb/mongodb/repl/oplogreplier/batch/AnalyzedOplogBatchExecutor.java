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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import com.eightkdata.mongowp.server.api.oplog.DbCmdOplogOperation;
import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.google.common.util.concurrent.Service;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.metrics.MetricNameFactory;
import com.torodb.core.metrics.ToroMetricRegistry;
import com.torodb.core.retrier.RetrierAbortException;
import com.torodb.core.retrier.RetrierGiveUpException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.mongodb.repl.oplogreplier.ApplierContext;
import com.torodb.mongodb.repl.oplogreplier.OplogOperationApplier.OplogApplyingException;

import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

/**
 *
 */
@ThreadSafe
public interface AnalyzedOplogBatchExecutor extends
    Service,
    AnalyzedOplogBatchVisitor<OplogOperation, ApplierContext, RetrierGiveUpException> {

  public void execute(OplogOperation op, ApplierContext context)
      throws OplogApplyingException, RollbackException, UserException;

  public void execute(CudAnalyzedOplogBatch cudBatch, ApplierContext context)
      throws RollbackException, UserException, NamespaceJobExecutionException;

  public default OplogOperation apply(AnalyzedOplogBatch batch,
      ApplierContext replContext) throws RetrierGiveUpException, RetrierAbortException {
    return batch.accept(this, replContext);
  }

  public static class AnalyzedOplogBatchExecutorMetrics {

    protected static final MetricNameFactory NAME_FACTORY =
        new MetricNameFactory("OplogBatchExecutor");
    private final ConcurrentMap<String, Timer> singleOpTimers = new ConcurrentHashMap<>();
    private final ToroMetricRegistry metricRegistry;
    private final Histogram cudBatchSize;
    private final Timer cudBatchTimer;
    private final Timer namespaceBatchTimer;

    @Inject
    public AnalyzedOplogBatchExecutorMetrics(ToroMetricRegistry metricRegistry) {
      this.metricRegistry = metricRegistry;
      this.cudBatchSize = metricRegistry.histogram(NAME_FACTORY.createMetricName("batchSize"));
      this.cudBatchTimer = metricRegistry.timer(NAME_FACTORY.createMetricName("cudTimer"));
      this.namespaceBatchTimer = metricRegistry.timer(NAME_FACTORY
          .createMetricName("namespaceTimer"));
    }

    /**
     * Returns the timer associated with {@link SingleOpAnalyzedOplogBatch} that contains the given
     * operation.
     *
     * @param singleOplogOp
     * @return
     */
    public Timer getSingleOpTimer(OplogOperation singleOplogOp) {
      String mapKey = getMapKey(singleOplogOp);
      return singleOpTimers.computeIfAbsent(
          mapKey,
          (key) -> createSingleTimer(singleOplogOp, key)
      );
    }

    public Histogram getCudBatchSize() {
      return cudBatchSize;
    }

    public Timer getCudBatchTimer() {
      return cudBatchTimer;
    }

    public Timer getNamespaceBatchTimer() {
      return namespaceBatchTimer;
    }

    @Nonnull
    private String getMapKey(OplogOperation oplogOp) {
      if (oplogOp instanceof DbCmdOplogOperation) {
        DbCmdOplogOperation cmdOp = (DbCmdOplogOperation) oplogOp;
        return cmdOp.getCommandName().orElse("unknownCmd");
      }
      return oplogOp.getType().name();
    }

    private Timer createSingleTimer(OplogOperation oplogOp, String mapKey) {
      String prefix = "single-";
      if (oplogOp instanceof DbCmdOplogOperation) {
        return metricRegistry.timer(NAME_FACTORY.createMetricName(prefix + mapKey));
      }
      return metricRegistry.timer(prefix + mapKey.toLowerCase(Locale.ENGLISH));
    }
  }

}
