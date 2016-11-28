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

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.torodb.core.annotations.TorodbRunnableService;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.services.RunnableTorodbService;
import com.torodb.core.supervision.Supervisor;
import com.torodb.core.transaction.RollbackException;
import com.torodb.mongodb.core.ExclusiveWriteMongodTransaction;
import com.torodb.mongodb.core.MongodConnection;
import com.torodb.mongodb.core.MongodServer;
import com.torodb.mongodb.repl.OplogManager;
import com.torodb.mongodb.repl.OplogManager.OplogManagerPersistException;
import com.torodb.mongodb.repl.OplogManager.WriteOplogTransaction;
import com.torodb.mongodb.repl.oplogreplier.OplogOperationApplier.OplogApplyingException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.ThreadFactory;

import javax.annotation.Nonnull;

class ReplSyncApplier extends RunnableTorodbService {

  private static final Logger LOGGER = LogManager.getLogger(ReplSyncApplier.class);
  private final SyncServiceView callback;
  private final OplogOperationApplier oplogOpApplier;
  private final OplogManager oplogManager;
  private final MongodConnection connection;

  private volatile Thread runThread;

  ReplSyncApplier(
      @TorodbRunnableService ThreadFactory threadFactory,
      @Nonnull OplogOperationApplier oplogOpApplier,
      @Nonnull MongodServer server,
      @Nonnull OplogManager oplogManager,
      @Nonnull SyncServiceView callback) {
    super(callback, threadFactory);
    this.callback = callback;
    this.connection = server.openConnection();
    this.oplogOpApplier = oplogOpApplier;
    this.oplogManager = oplogManager;
  }

  @Override
  protected Logger getLogger() {
    return LOGGER;
  }

  @Override
  protected String serviceName() {
    return "ToroDB Sync Applier";
  }

  @Override
  protected void triggerShutdown() {
    if (runThread != null) {
      runThread.interrupt();
    }
  }

  @Override
  protected void runProtected() {
    runThread = Thread.currentThread();
    /*
     * TODO: In general, the replication context can be set as not reaplying. But it is not frequent
     * but possible to stop the replication after some oplog ops have been apply but not marked as
     * executed on the oplog manager. For that reason, all oplog ops betwen the last operation that
     * have been marked as applyed and the current last operation on the remote oplog must be
     * executed as replying operations. As it is not possible to do that yet, we have to always
     * apply operations as replying to be safe.
     */
    ApplierContext applierContext = new ApplierContext.Builder()
        .setReapplying(true)
        .setUpdatesAsUpserts(true)
        .build();
    while (isRunning()) {
      OplogOperation lastOperation = null;
      ExclusiveWriteMongodTransaction trans = connection.openExclusiveWriteTransaction();
      try (ExclusiveWriteMongodTransaction transaction = trans) {
        try {
          for (OplogOperation opToApply : callback.takeOps()) {
            lastOperation = opToApply;
            LOGGER.trace("Executing {}", opToApply);
            try {
              boolean done = false;
              while (!done) {
                try {
                  oplogOpApplier.apply(
                      opToApply,
                      transaction,
                      applierContext
                  );
                  transaction.commit();
                  done = true;
                } catch (RollbackException ex) {
                  LOGGER.debug("Recived a rollback exception while applying an oplog op", ex);
                }
              }
            } catch (OplogApplyingException ex) {
              if (!callback.failedToApply(opToApply, ex)) {
                LOGGER.error(serviceName() + " stopped because one operation "
                    + "cannot be executed", ex);
                break;
              }
            } catch (UserException ex) {
              if (callback.failedToApply(opToApply, ex)) {

                LOGGER.error(serviceName() + " stopped because one operation "
                    + "cannot be executed", ex);
                break;
              }
            } catch (Throwable ex) {
              if (callback.failedToApply(opToApply, ex)) {
                LOGGER.error(serviceName() + " stopped because "
                    + "an unknown error", ex);
                break;
              }
            }
            callback.markAsApplied(opToApply);
          }
        } catch (InterruptedException ex) {
          LOGGER.debug("Interrupted applier thread while applying an operator");
        }
      }
      if (lastOperation != null) {
        try (WriteOplogTransaction oplogTransaction =
            oplogManager.createWriteTransaction()) {
          oplogTransaction.addOperation(lastOperation);
        } catch (OplogManagerPersistException ex) {
          if (callback.failedToApply(lastOperation, ex)) {
            LOGGER.error(serviceName() + " stopped because "
                + "the last applied operation couldn't "
                + "be persisted", ex);
            break;
          }
        }
      }
    }
  }

  @Override
  protected void shutDown() throws Exception {
    connection.close();
  }

  public static interface SyncServiceView extends Supervisor {

    public List<OplogOperation> takeOps() throws InterruptedException;

    public void markAsApplied(OplogOperation oplogOperation);

    /**
     *
     * @param oplogOperation
     * @param status
     * @return false iff the applier loop should stop
     */
    public boolean failedToApply(OplogOperation oplogOperation, Status<?> status);

    /**
     *
     * @param oplogOperation
     * @param t
     * @return false iff the applier loop should stop
     */
    public boolean failedToApply(OplogOperation oplogOperation, OplogManagerPersistException t);

    /**
     *
     * @param oplogOperation
     * @param t
     * @return false iff the applier loop should stop
     */
    public boolean failedToApply(OplogOperation oplogOperation, Throwable t);

  }
}
