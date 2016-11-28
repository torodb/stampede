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

import com.torodb.concurrent.ExecutorServiceShutdownHelper;
import com.torodb.core.annotations.TorodbIdleService;
import com.torodb.core.concurrent.ConcurrentToolsFactory;
import com.torodb.core.services.IdleTorodbService;
import com.torodb.core.supervision.Supervisor;
import com.torodb.mongodb.commands.pojos.MemberState;
import com.torodb.mongodb.repl.guice.MongoDbRepl;
import com.torodb.mongodb.repl.oplogreplier.OplogApplierService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.lambda.fi.util.function.CheckedFunction;
import org.jooq.lambda.fi.util.function.CheckedSupplier;

import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

@ThreadSafe
public class ReplCoordinatorStateMachine extends IdleTorodbService {

  private static final Logger LOGGER =
      LogManager.getLogger(ReplCoordinatorStateMachine.class);
  private static final String THREAD_PREFIX = "repl-coord-";
  private final ExecutorServiceShutdownHelper shutdownHelper;
  private final ConcurrentToolsFactory concurrentToolsFactory;
  private final ReplMetrics metrics;
  private final RecoveryService.RecoveryServiceFactory recoveryServiceFactory;
  private final OplogApplierService.OplogApplierServiceFactory oplogReplierFactory;
  private final Supervisor supervisor;

  private ExecutorService executorService;
  @Nonnull
  private ReplCoordinatorState state;
  private RecoveryService recoveryService;
  private OplogApplierService oplogReplierService;

  @Inject
  public ReplCoordinatorStateMachine(
      @TorodbIdleService ThreadFactory threadFactory,
      @MongoDbRepl Supervisor supervisor,
      ConcurrentToolsFactory concurrentToolsFactory,
      ExecutorServiceShutdownHelper shutdownHelper,
      RecoveryService.RecoveryServiceFactory recoveryServiceFactory,
      OplogApplierService.OplogApplierServiceFactory oplogReplierFactory,
      ReplMetrics metrics) {
    super(threadFactory);
    this.shutdownHelper = shutdownHelper;
    this.state = ReplCoordinatorState.STARTUP;
    this.recoveryServiceFactory = recoveryServiceFactory;
    this.oplogReplierFactory = oplogReplierFactory;
    this.metrics = metrics;
    this.supervisor = supervisor;
    this.concurrentToolsFactory = concurrentToolsFactory;
  }

  @Override
  protected void startUp() throws Exception {
    this.executorService = concurrentToolsFactory
        .createExecutorServiceWithMaxThreads(THREAD_PREFIX + "idle", 1);
    CompletableFuture.runAsync(() -> setState(ReplCoordinatorState.IDLE),
        executorService).join();
  }

  @Override
  protected void shutDown() throws Exception {
    CompletableFuture.runAsync(this::shutDownPrivate, executorService).join();
    shutdownHelper.shutdown(executorService);
  }

  private void shutDownPrivate() {
    switch (state) {
      case RECOVERY:
        stopRecoveryModePrivate();
        break;
      case SECONDARY:
        stopSecondaryModePrivate();
        break;
      default:
        break;
    }
    setState(ReplCoordinatorState.TERMINATED);
  }

  CompletableFuture<StateChange> fromRecoveryToSecondary(OplogApplierService.Callback callback) {
    return CompletableFuture.supplyAsync(
        () -> fromRecoveryToSecondaryPrivate(callback),
        executorService
    );
  }

  CompletableFuture<StateChange> fromSecondaryToRecovery(RecoveryService.Callback callback) {
    return CompletableFuture.supplyAsync(
        () -> fromSecondaryToRecoveryPrivate(callback),
        executorService
    );
  }

  CompletableFuture<StateChange> startRecoveryMode(
      RecoveryService.Callback serviceCallback) {
    return CompletableFuture.supplyAsync(
        () -> startModeWrapper(
            ReplCoordinatorState.RECOVERY,
            serviceCallback,
            this::startRecoveryModePrivate
        ),
        executorService
    );
  }

  CompletableFuture<StateChange> stopRecoveryMode() {
    return CompletableFuture.supplyAsync(
        () -> stopModeWrapper(
            ReplCoordinatorState.RECOVERY,
            this::stopRecoveryModePrivate
        ),
        executorService
    );
  }

  CompletableFuture<StateChange> startSecondaryMode(
      OplogApplierService.Callback serviceCallback) {
    return CompletableFuture.supplyAsync(
        () -> startModeWrapper(
            ReplCoordinatorState.SECONDARY,
            serviceCallback,
            this::startSecondaryModePrivate
        ),
        executorService
    );

  }

  CompletableFuture<StateChange> stopSecondaryMode() {
    return CompletableFuture.supplyAsync(
        () -> stopModeWrapper(
            ReplCoordinatorState.SECONDARY,
            this::stopSecondaryModePrivate
        ),
        executorService
    );
  }

  /**
   * Changes the name of the current thread and returns the older one.
   *
   * @param postfix
   * @return
   */
  private String changeThreadName(String postfix) {
    Thread currentThread = Thread.currentThread();
    String oldThreadName = currentThread.getName();
    currentThread.setName(THREAD_PREFIX + postfix);
    return oldThreadName;
  }

  private void restoreThreadName(String oldName) {
    Thread currentThread = Thread.currentThread();
    currentThread.setName(oldName);
  }

  StateChange fromRecoveryToSecondaryPrivate(OplogApplierService.Callback callback) {
    StateChange result;

    result = stopModeWrapper(
        ReplCoordinatorState.RECOVERY,
        this::stopRecoveryModePrivate
    );
    if (result.success()) {
      result = startModeWrapper(
          ReplCoordinatorState.SECONDARY,
          callback,
          this::startSecondaryModePrivate
      );
    }
    return result;
  }

  StateChange fromSecondaryToRecoveryPrivate(RecoveryService.Callback callback) {
    StateChange result;

    result = stopModeWrapper(
        ReplCoordinatorState.SECONDARY,
        this::stopSecondaryModePrivate
    );
    if (result.success()) {
      result = startModeWrapper(
          ReplCoordinatorState.RECOVERY,
          callback,
          this::startRecoveryModePrivate
      );
    }
    return result;
  }

  /**
   * A wrapper method that, given the required information to transist to a new state, checks
   * preconditions and set several generic things.
   *
   * @param <C>
   * @param triedState         the state is trying to be started
   * @param callback           the argument (usually a callback) the new state requires
   * @param startStateFunction the function that starts the new state
   * @return
   */
  private <C> StateChange startModeWrapper(ReplCoordinatorState triedState,
      C callback,
      CheckedFunction<C, StateChange> startStateFunction) {

    String oldThreadName = changeThreadName("starting-" + triedState.name()
        .toLowerCase(Locale.ENGLISH));
    ReplCoordinatorState oldState = state;
    try {
      if (oldState == ReplCoordinatorState.IDLE) {
        LOGGER.info("Starting {} mode", triedState.name()
            .toUpperCase(Locale.ENGLISH));
        return startStateFunction.apply(callback);
      }
      switch (oldState) {
        default:
          throw new AssertionError("Unexpected "
              + ReplCoordinatorState.class.getSimpleName() + ": "
              + oldState);
        case STARTUP:
          LOGGER.debug("Trying to start the mode {} when the "
              + "current mode is {}. {} service must be started "
              + "before any change are accepted.", triedState,
              oldState, serviceName());
          break;
        case RECOVERY:
        case SECONDARY:
          LOGGER.debug("Trying to start the mode {} when the "
              + "current mode is {}. Stop that state before "
              + "trying to change it", triedState, oldState);
          break;
        case TERMINATED:
          LOGGER.debug("Trying to start the mode {} when the "
              + "current mode is {}. No more state changes are "
              + "acepted ", triedState, oldState);
          break;
      }
      return new StateChange(oldState, triedState,
          new RejectionCause(RejectionType.ILLEGAL_CHANGE));
    } catch (Throwable ex) {
      LOGGER.warn("Unexpected error while being on " + state + " state "
          + "and trying to start " + triedState, ex);

      setState(ReplCoordinatorState.ERROR);
      supervisor.onError(this, ex);

      return new StateChange(oldState, triedState, state,
          new RejectionCause(RejectionType.UNEXPECTED_ERROR, ex));
    } finally {
      restoreThreadName(oldThreadName);
    }
  }

  private StateChange stopModeWrapper(ReplCoordinatorState toStopState,
      CheckedSupplier<StateChange> stopStateFunction) {
    String oldThreadName = changeThreadName("stopping-" + toStopState.name()
        .toLowerCase(Locale.ENGLISH));

    ReplCoordinatorState oldState = state;
    try {
      if (state == toStopState) {
        LOGGER.info("Stopping {} mode", toStopState.name()
            .toUpperCase(Locale.ENGLISH));
        return stopStateFunction.get();
      } else {
        LOGGER.debug("Trying to stop the state {} while being on "
            + "state {}", toStopState, state);
        return new StateChange(oldState, toStopState,
            new RejectionCause(RejectionType.ILLEGAL_CHANGE));
      }
    } catch (Throwable ex) {
      LOGGER.debug("Unexpected error while being on " + state + " state"
          + " and trying to stop it", ex);

      setState(ReplCoordinatorState.ERROR);
      supervisor.onError(this, ex);

      return new StateChange(oldState, toStopState, state,
          new RejectionCause(RejectionType.UNEXPECTED_ERROR, ex));
    } finally {
      restoreThreadName(oldThreadName);
    }
  }

  private StateChange startRecoveryModePrivate(
      RecoveryService.Callback serviceCallback) {
    assert state == ReplCoordinatorState.RECOVERY;
    assert oplogReplierService == null || !oplogReplierService.isRunning();

    final ReplCoordinatorState triedState = ReplCoordinatorState.RECOVERY;

    recoveryService = recoveryServiceFactory
        .createRecoveryService(serviceCallback);
    recoveryService.startAsync();
    recoveryService.awaitRunning();

    setState(triedState);

    return new StateChange(ReplCoordinatorState.RECOVERY, triedState);
  }

  private StateChange startSecondaryModePrivate(
      OplogApplierService.Callback serviceCallback) {
    assert state == ReplCoordinatorState.SECONDARY;
    assert recoveryService == null || !recoveryService.isRunning();

    final ReplCoordinatorState triedState = ReplCoordinatorState.SECONDARY;

    oplogReplierService = oplogReplierFactory
        .createOplogApplier(serviceCallback);
    oplogReplierService.startAsync();
    oplogReplierService.awaitRunning();

    setState(triedState);

    return new StateChange(ReplCoordinatorState.SECONDARY, triedState);
  }

  private StateChange stopRecoveryModePrivate() {
    assert state == ReplCoordinatorState.RECOVERY;
    LOGGER.debug("Shutting down recovery service");
    recoveryService.stopAsync();
    recoveryService.awaitTerminated();
    LOGGER.debug("Recovery service has been shutted down");

    recoveryService = null;
    setState(ReplCoordinatorState.IDLE);
    return new StateChange(ReplCoordinatorState.RECOVERY, ReplCoordinatorState.IDLE);
  }

  private StateChange stopSecondaryModePrivate() {
    assert state == ReplCoordinatorState.SECONDARY;
    LOGGER.debug("Shutting down secondary service");
    oplogReplierService.stopAsync();
    oplogReplierService.awaitTerminated();
    LOGGER.debug("Secondary service has been shutted down");

    oplogReplierService = null;
    setState(ReplCoordinatorState.IDLE);
    return new StateChange(ReplCoordinatorState.SECONDARY, ReplCoordinatorState.IDLE);
  }

  private void setState(@Nonnull ReplCoordinatorState state) {
    this.state = state;

    MemberState rsMemberState;

    switch (state) {
      case RECOVERY:
        assert recoveryService != null;
        assert oplogReplierService == null;
        rsMemberState = MemberState.RS_RECOVERING;
        break;
      case SECONDARY:
        assert recoveryService == null;
        assert oplogReplierService != null;
        rsMemberState = MemberState.RS_SECONDARY;
        break;
      default:
        assert recoveryService == null;
        assert oplogReplierService == null;
        rsMemberState = MemberState.RS_UNKNOWN;
    }

    metrics.getMemberState().setValue(rsMemberState.name());
    metrics.getMemberStateCounters().get(rsMemberState).inc();
  }

  public static class StateChange {

    private final ReplCoordinatorState oldState;
    private final ReplCoordinatorState triedState;
    private final ReplCoordinatorState newState;
    private final Optional<RejectionCause> rejectionCause;

    public StateChange(ReplCoordinatorState oldState,
        ReplCoordinatorState triedState) {
      assert oldState != triedState : "There was not change";
      this.oldState = oldState;
      this.triedState = triedState;
      this.newState = triedState;
      this.rejectionCause = Optional.empty();
    }

    public StateChange(ReplCoordinatorState oldState, ReplCoordinatorState triedState,
        RejectionCause rejectionCause) {
      this.oldState = oldState;
      this.newState = oldState;
      this.triedState = triedState;
      this.rejectionCause = Optional.of(rejectionCause);
    }

    public StateChange(ReplCoordinatorState oldState,
        ReplCoordinatorState triedState,
        ReplCoordinatorState newState,
        RejectionCause rejectionCause) {
      this.oldState = oldState;
      this.triedState = triedState;
      this.newState = newState;
      this.rejectionCause = Optional.of(rejectionCause);
    }

    ReplCoordinatorState getOldState() {
      return oldState;
    }

    ReplCoordinatorState getNewState() {
      return newState;
    }

    ReplCoordinatorState getTriedState() {
      return triedState;
    }

    Optional<RejectionCause> getRejectionCause() {
      return rejectionCause;
    }

    boolean hasChanged() {
      return getOldState() != getNewState();
    }

    boolean onTriedState() {
      return getNewState() == getTriedState();
    }

    boolean success() {
      return !getRejectionCause().isPresent();
    }
  }

  public static enum RejectionType {
    /**
     * The change that was tried to be applied is not valid on the current state.
     */
    ILLEGAL_CHANGE,
    /**
     * The state that was tried to be applied is the same than the older one.
     */
    NO_CHANGE,
    /**
     * It was impossible to start the new state because an unexpected error happened.
     */
    UNEXPECTED_ERROR,
    /**
     * An error happened when trying to start the new state.
     */
    CANNOT_START_NEW_STATE,
    /**
     * An error happened when trying to stop the old state.
     */
    CANNOT_STOP_OLD_STATE;
  }

  public static class RejectionCause {

    private final RejectionType rejectionType;
    private final Optional<Throwable> cause;

    public RejectionCause(RejectionType rejectionType) {
      this.rejectionType = rejectionType;
      this.cause = Optional.empty();
    }

    public RejectionCause(RejectionType rejectionType, @Nullable Throwable cause) {
      this.rejectionType = rejectionType;
      this.cause = Optional.ofNullable(cause);
    }

    public RejectionType getRejectionType() {
      return rejectionType;
    }

    public Optional<Throwable> getCause() {
      return cause;
    }
  }

}
