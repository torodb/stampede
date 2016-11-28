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

package com.torodb.mongodb.repl.guice;

import com.torodb.core.annotations.ParallelLevel;
import com.torodb.core.concurrent.ConcurrentToolsFactory;
import com.torodb.core.retrier.Retrier;
import com.torodb.mongodb.utils.cloner.AkkaDbCloner;
import com.torodb.mongodb.utils.cloner.CommitHeuristic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Clock;
import java.util.concurrent.ThreadFactory;

import javax.inject.Inject;
import javax.inject.Provider;

/**
 *
 */
public class AkkaDbClonerProvider implements Provider<AkkaDbCloner> {

  private static final Logger LOGGER = LogManager.getLogger(AkkaDbClonerProvider.class);

  private final ThreadFactory threadFactory;
  private final ConcurrentToolsFactory concurrentToolsFactory;
  private final int parallelLevel;
  private final int docsPerTransaction;
  private final CommitHeuristic commitHeuristic;
  private final Clock clock;
  private final Retrier retrier;

  /**
   *
   * @param threadFactory
   * @param concurrentToolsFactory
   * @param parallelLevel
   * @param docsPerTransaction
   * @param commitHeuristic
   * @param clock
   * @param retrier
   */
  @Inject
  public AkkaDbClonerProvider(ThreadFactory threadFactory,
      ConcurrentToolsFactory concurrentToolsFactory,
      @ParallelLevel int parallelLevel,
      @DocsPerTransaction int docsPerTransaction,
      CommitHeuristic commitHeuristic, Clock clock, Retrier retrier) {
    this.threadFactory = threadFactory;
    this.concurrentToolsFactory = concurrentToolsFactory;
    this.parallelLevel = parallelLevel;
    this.commitHeuristic = commitHeuristic;
    this.clock = clock;
    this.docsPerTransaction = docsPerTransaction;
    this.retrier = retrier;
  }

  @Override
  public AkkaDbCloner get() {
    LOGGER.debug("Using AkkaDbCloner with: {parallelLevel: {}, docsPerTransaction: {}}",
        parallelLevel, docsPerTransaction);
    return new AkkaDbCloner(
        threadFactory,
        concurrentToolsFactory,
        Math.max(1, parallelLevel - 1),
        parallelLevel * docsPerTransaction,
        commitHeuristic,
        clock,
        retrier
    );
  }

}
