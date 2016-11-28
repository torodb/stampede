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

package com.torodb.mongodb.repl;

import com.google.common.base.Throwables;
import com.torodb.core.annotations.TorodbIdleService;
import com.torodb.core.services.IdleTorodbService;
import com.torodb.core.supervision.EscalatingException;
import com.torodb.core.supervision.Supervisor;
import com.torodb.core.supervision.SupervisorDecision;
import com.torodb.mongodb.repl.guice.MongoDbRepl;
import com.torodb.mongodb.repl.oplogreplier.OplogApplierService;
import com.torodb.mongodb.repl.oplogreplier.RollbackReplicationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ThreadFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

/**
 *
 */
@ThreadSafe
public class ReplCoordinator extends IdleTorodbService implements Supervisor {

  private static final Logger LOGGER =
      LogManager.getLogger(ReplCoordinator.class);
  private final ConsistencyHandler consistencyHandler;
  private final Supervisor replSupervisor;
  private final ReplCoordinatorStateMachine stateMachine;

  @Inject
  public ReplCoordinator(
      @TorodbIdleService ThreadFactory threadFactory,
      ConsistencyHandler consistencyHandler,
      @MongoDbRepl Supervisor replSupervisor,
      ReplCoordinatorStateMachine stateMachine) {
    super(threadFactory);
    this.consistencyHandler = consistencyHandler;
    this.replSupervisor = replSupervisor;
    this.stateMachine = stateMachine;
  }

  @Override
  protected void startUp() throws Exception {
    LOGGER.debug("Starting replication coordinator");
    stateMachine.startAsync();
    stateMachine.awaitRunning();

    loadStoredConfig();

    if (!consistencyHandler.isConsistent()) {
      LOGGER.info("Database is not consistent.");
      stateMachine.startRecoveryMode(new RecoveryServiceCallback());
    } else {
      LOGGER.info("Database is consistent.");
      stateMachine.startSecondaryMode(new OplogReplierServiceCallback());
    }
    LOGGER.debug("Replication coordinator started");
  }

  @Override
  protected void shutDown() throws Exception {
    LOGGER.debug("Shutting down replication coordinator");
    stateMachine.stopAsync();
    stateMachine.awaitTerminated();
    LOGGER.debug("Replication coordinator shutted down");
  }

  @Override
  public SupervisorDecision onError(Object supervised, Throwable error) {
    SupervisorDecision decision = replSupervisor.onError(
        this,
        new EscalatingException(replSupervisor, this, supervised, error)
    );
    if (decision == SupervisorDecision.STOP) {
      this.stopAsync();
    }
    return decision;
  }

  private void loadStoredConfig() {
    LOGGER.warn("loadStoredConfig() is not implemented yet");
  }

  private class RecoveryServiceCallback implements RecoveryService.Callback {

    @Override
    public void waitUntilStartPermision() {
      ReplCoordinator.this.awaitRunning();
    }

    @Override
    public void recoveryFinished(RecoveryService service) {
      LOGGER.debug("Recovery finishes");
      stateMachine.fromRecoveryToSecondary(
          new OplogReplierServiceCallback()
      );
    }

    @Override
    public void recoveryFailed(RecoveryService service, Throwable ex) {
      Throwable cause = Throwables.getRootCause(ex);
      LOGGER
          .error("Fatal error while starting recovery mode: " + cause.getLocalizedMessage(), cause);
      ReplCoordinator.this.onError(service, ex);
    }

    @Override
    public void recoveryFailed(RecoveryService service) {
      LOGGER.error("Fatal error while starting recovery mode");
      ReplCoordinator.this.onError(service, new AssertionError(
          "Recovery finished before it was expected"));
    }

    @Override
    public void setConsistentState(boolean consistent) {
      try {
        consistencyHandler.setConsistent(consistent);
      } catch (Throwable ex) {
        throw new AssertionError("Fatal error: It was impossible to "
            + "store the consistent state", ex);
      }
    }

    @Override
    public boolean canAcceptWrites(String database) {
      return true;
    }
  }

  private class OplogReplierServiceCallback implements OplogApplierService.Callback {

    private volatile boolean shuttingUp = false;

    @Override
    public void waitUntilStartPermision() {
      ReplCoordinator.this.awaitRunning();
    }

    @Override
    public void rollback(OplogApplierService oplogApplierService,
        RollbackReplicationException t) {
      LOGGER.debug("Secondary request a rollback with an exception", t);
      LOGGER.debug("ROLLBACK state is ignored, delegating on RECOVERY");
      shuttingUp = true;
      stateMachine.fromSecondaryToRecovery(new RecoveryServiceCallback());
    }

    @Override
    public void onFinish(OplogApplierService oplogApplierService) {
      if (!shuttingUp && isRunning()) {
        ReplCoordinator.this.onError(oplogApplierService,
            new IllegalStateException("Unexpected oplog applier "
                + "service shutdown"));
      } else {
        stateMachine.stopSecondaryMode();
      }
    }

    @Override
    public void onError(OplogApplierService oplogApplierService,
        Throwable t) {
      ReplCoordinator.this.onError(oplogApplierService, t);
    }
  }

}
