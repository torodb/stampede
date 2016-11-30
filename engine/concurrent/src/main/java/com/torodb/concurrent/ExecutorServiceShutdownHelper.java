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

package com.torodb.concurrent;

import com.torodb.core.Shutdowner;
import com.torodb.core.Shutdowner.ShutdownListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

/**
 *
 */
public class ExecutorServiceShutdownHelper {

  private static final Logger LOGGER = LogManager.getLogger(ExecutorServiceShutdownHelper.class);
  private final Shutdowner shutdowner;
  private final Clock clock;

  @Inject
  public ExecutorServiceShutdownHelper(Shutdowner shutdowner, Clock clock) {
    this.shutdowner = shutdowner;
    this.clock = clock;
  }

  public void terminateOnShutdown(String executorServiceName,
      ExecutorService executorService) {
    shutdowner.addShutdownListener(executorService,
        new ExecutorServiceShutdowner(executorServiceName));
  }

  public void shutdown(ExecutorService executorService) throws InterruptedException {
    Instant start = clock.instant();
    executorService.shutdown();
    boolean terminated = false;
    int waitTime = 1;
    while (!terminated) {
      terminated = executorService.awaitTermination(waitTime, TimeUnit.SECONDS);
      if (!terminated) {
        LOGGER.info("ExecutorService {} did not finished in {}",
            executorService,
            Duration.between(start, clock.instant()));
      }
    }
  }

  private class ExecutorServiceShutdowner implements ShutdownListener<ExecutorService> {

    private final String executorServiceName;

    public ExecutorServiceShutdowner(String executorServiceName) {
      this.executorServiceName = executorServiceName;
    }

    @Override
    public void onShutdown(ExecutorService e) throws Exception {
      ExecutorServiceShutdownHelper.this.shutdown(e);
    }

    @Override
    public String describeResource(ExecutorService resource) {
      return executorServiceName + " executor service";
    }
  }

}
