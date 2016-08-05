/*
 * This file is part of ToroDB.
 *
 * ToroDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ToroDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with repl. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.mongodb.repl.guice;

import com.torodb.core.annotations.ParallelLevel;
import com.torodb.core.concurrent.StreamExecutor;
import com.torodb.mongodb.utils.cloner.AkkaDbCloner;
import com.torodb.mongodb.utils.cloner.CommitHeuristic;
import java.time.Clock;
import javax.inject.Inject;
import javax.inject.Provider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.torodb.core.concurrent.ToroDbExecutorService;
import com.torodb.core.retrier.Retrier;

/**
 *
 */
public class AkkaDbClonerProvider implements Provider<AkkaDbCloner> {
    private static final Logger LOGGER = LogManager.getLogger(AkkaDbClonerProvider.class);

    private final ToroDbExecutorService executor;
    private final StreamExecutor streamExecutor;
    private final int parallelLevel;
    private final int docsPerTransaction;
    private final CommitHeuristic commitHeuristic;
    private final Clock clock;
    private final Retrier retrier;

    /**
     *
     * @param executor
     * @param streamExecutor
     * @param parallelLevel
     * @param docsPerTransaction
     * @param commitHeuristic
     * @param clock
     * @param retrier
     */
    @Inject
    public AkkaDbClonerProvider(ToroDbExecutorService executor, StreamExecutor streamExecutor,
            @ParallelLevel int parallelLevel,  @DocsPerTransaction int docsPerTransaction,
            CommitHeuristic commitHeuristic, Clock clock, Retrier retrier) {
        this.executor = executor;
        this.streamExecutor = streamExecutor;
        this.parallelLevel = parallelLevel;
        this.commitHeuristic = commitHeuristic;
        this.clock = clock;
        this.docsPerTransaction = docsPerTransaction;
        this.retrier = retrier;
    }

    @Override
    public AkkaDbCloner get() {
        LOGGER.info("Using AkkaDbCloner with: {parallelLevel: {}, docsPerTransaction: {}}",
                parallelLevel, docsPerTransaction);
        return new AkkaDbCloner(
                executor,
                parallelLevel - 1,
                streamExecutor,
                parallelLevel * docsPerTransaction,
                docsPerTransaction,
                commitHeuristic,
                clock,
                retrier
        );
    }

}