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

package com.torodb.core.services;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.torodb.core.supervision.SupervisedService;
import com.torodb.core.supervision.Supervisor;
import com.torodb.core.supervision.SupervisorDecision;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;

/**
 *
 */
public abstract class RunnableTorodbService extends AbstractExecutionThreadService
    implements Supervisor, SupervisedService {

  private final Supervisor supervisor;
  private final ThreadFactory threadFactory;

  public RunnableTorodbService(Supervisor supervisor, ThreadFactory threadFactory) {
    this.supervisor = supervisor;
    this.threadFactory = threadFactory;
  }

  protected abstract Logger getLogger();

  @Override
  protected Executor executor() {
    return (Runnable command) -> {
      Thread thread = threadFactory.newThread(command);
      thread.start();
    };
  }

  /**
   * Executes the service.
   *
   * Any throwable thrown by this methos will be reported to the {@link #getSupervisor() supervisor}
   * of this service
   *
   * @throws Exception
   */
  protected abstract void runProtected() throws Exception;

  @Override
  protected final void run() throws Exception {
    try {
      runProtected();
    } catch (Throwable ex) {
      SupervisorDecision decision = escalateError(ex);
      getLogger().debug("Error while executiong {}. Ignoring supervision "
          + "decision {} because this runnable service will be "
          + "stopped anyway", serviceName(), decision);
    }
  }

  @Override
  public Supervisor getSupervisor() {
    return supervisor;
  }

  @Override
  public SupervisorDecision onError(Object supervised, Throwable error) {
    return escalateError(error);
  }

  public SupervisorDecision escalateError(Throwable error) {
    getLogger().trace("Escalating error to {}", getSupervisor());
    SupervisorDecision decision = getSupervisor().onError(this, error);
    executeSupervisorDecision(decision);
    return decision;
  }

  protected void executeSupervisorDecision(SupervisorDecision decision) {
    getLogger().debug("Executing the supervisor decision {}", decision);
    switch (decision) {
      case STOP: {
        this.stopAsync();
        break;
      }
      case IGNORE:
      default:
    }
  }

}
