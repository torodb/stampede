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

package com.torodb.standalone;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import com.torodb.core.Shutdowner;
import com.torodb.core.backend.BackendBundle;
import com.torodb.core.backend.BackendBundleFactory;
import com.torodb.core.supervision.Supervisor;
import com.torodb.core.supervision.SupervisorDecision;
import com.torodb.torod.TorodBundle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;

/**
 *
 */
public class ToroDbService extends AbstractIdleService implements Supervisor {

  private static final Logger LOGGER =
      LogManager.getLogger(ToroDbService.class);
  private final ThreadFactory threadFactory;
  private final Injector bootstrapInjector;
  private Shutdowner shutdowner;

  public ToroDbService(ThreadFactory threadFactory, Injector bootstrapInjector) {
    this.threadFactory = threadFactory;
    this.bootstrapInjector = bootstrapInjector;
  }

  @Override
  protected Executor executor() {
    return (Runnable command) -> {
      Thread thread = threadFactory.newThread(command);
      thread.start();
    };
  }

  @Override
  public SupervisorDecision onError(Object supervised, Throwable error) {
    this.stopAsync();
    return SupervisorDecision.STOP;
  }

  @Override
  protected void startUp() throws Exception {
    LOGGER.info("Starting up ToroDB Standalone");

    shutdowner = bootstrapInjector.getInstance(Shutdowner.class);

    BackendBundle backendBundle = createBackendBundle();
    startBundle(backendBundle);

    Injector finalInjector = createFinalInjector(backendBundle);

    TorodBundle torodBundle = createTorodBundle(finalInjector);
    startBundle(torodBundle);

    LOGGER.info("ToroDB Stampede is now running");
  }

  @Override
  protected void shutDown() throws Exception {
    LOGGER.info("Shutting down ToroDB Standalone");
    if (shutdowner != null) {
      shutdowner.stopAsync();
      shutdowner.awaitTerminated();
    }
    LOGGER.info("ToroDB Stampede has been shutted down");
  }

  private BackendBundle createBackendBundle() {
    return bootstrapInjector.getInstance(BackendBundleFactory.class)
        .createBundle(this);
  }

  protected Injector createFinalInjector(BackendBundle backendBundle) {
    ToroDbRuntimeModule runtimeModule = new ToroDbRuntimeModule(
        backendBundle, this);
    return bootstrapInjector.createChildInjector(runtimeModule);
  }

  private TorodBundle createTorodBundle(Injector finalInjector) {
    return finalInjector.getInstance(TorodBundle.class);
  }

  private void startBundle(Service service) {
    service.startAsync();
    service.awaitRunning();

    shutdowner.addStopShutdownListener(service);
  }

}
