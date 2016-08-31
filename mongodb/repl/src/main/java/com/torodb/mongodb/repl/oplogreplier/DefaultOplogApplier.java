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

package com.torodb.mongodb.repl.oplogreplier;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.dispatch.ExecutionContexts;
import akka.japi.Pair;
import akka.japi.function.Creator;
import akka.stream.*;
import akka.stream.javadsl.*;
import akka.stream.stage.*;
import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.google.common.base.Supplier;
import com.torodb.core.Shutdowner;
import com.torodb.core.concurrent.ToroDbExecutorService;
import com.torodb.mongodb.repl.OplogManager;
import com.torodb.mongodb.repl.OplogManager.OplogManagerPersistException;
import com.torodb.mongodb.repl.OplogManager.WriteOplogTransaction;
import com.torodb.mongodb.repl.oplogreplier.batch.AnalyzedOplogBatch;
import com.torodb.mongodb.repl.oplogreplier.batch.AnalyzedOplogBatchExecutor;
import com.torodb.mongodb.repl.oplogreplier.batch.BatchAnalyzer;
import com.torodb.mongodb.repl.oplogreplier.batch.BatchAnalyzer.BatchAnalyzerFactory;
import com.torodb.mongodb.repl.oplogreplier.fetcher.OplogFetcher;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;


/**
 *
 */
public class DefaultOplogApplier implements OplogApplier {

    private static final Logger LOGGER = LogManager.getLogger(DefaultOplogApplier.class);
    private final BatchLimits batchLimits;
    private final AnalyzedOplogBatchExecutor batchExecutor;
    private final OplogManager oplogManager;
    private final BatchAnalyzerFactory batchAnalyzerFactory;
    private final ActorSystem actorSystem;
    private final ToroDbExecutorService executor;

    @Inject
    public DefaultOplogApplier(BatchLimits batchLimits, OplogManager oplogManager,
            AnalyzedOplogBatchExecutor batchExecutor, BatchAnalyzerFactory batchAnalyzerFactory,
            ToroDbExecutorService executor, Shutdowner shutdowner) {
        this.batchExecutor = batchExecutor;
        this.batchLimits = batchLimits;
        this.oplogManager = oplogManager;
        this.batchAnalyzerFactory = batchAnalyzerFactory;
        this.executor = executor;
        this.actorSystem = ActorSystem.create("oplogReplier", null, null,
                ExecutionContexts.fromExecutor(executor)
        );
        shutdowner.addCloseShutdownListener(this);
    }

    @Override
    public ApplyingJob apply(OplogFetcher fetcher, ApplierContext applierContext) {

        Materializer materializer = ActorMaterializer.create(actorSystem);

        RunnableGraph<Pair<UniqueKillSwitch, CompletionStage<Done>>> graph = createOplogSource(() -> fetcher)
                .via(createBatcherFlow(applierContext))
                .viaMat(KillSwitches.single(), Keep.right())
                .async()
                .map(analyzedOp -> batchExecutor.apply(analyzedOp, applierContext))
                .toMat(
                        Sink.foreach(this::storeLastAppliedOp),
                        (_killSwitch, completionStage) -> new Pair<>(_killSwitch, completionStage)
                );

        Pair<UniqueKillSwitch, CompletionStage<Done>> pair = graph.run(materializer);
        UniqueKillSwitch killSwitch = pair.first();

        CompletableFuture<Empty> whenComplete = pair.second().toCompletableFuture()
                .thenApply(done -> Empty.getInstance())
                .whenComplete((done, t) -> {
                    fetcher.close();
                    if (done != null) {
                        LOGGER.trace("Oplog replication stream finished normally");
                    } else {
                        Throwable cause;
                        if (t instanceof CompletionException) {
                            cause = t.getCause();
                        } else {
                            cause = t;
                        }
                        if (cause instanceof CancellationException) { //the completable future has been cancelled
                            LOGGER.debug("Oplog replication stream has been cancelled");
                            killSwitch.abort(cause); //to stop the stream on an external cancellation
                        } else { //in this case the exception should came from the stream
                            LOGGER.error("Oplog replication stream finished exceptionally", cause);
                            //the stream should be finished exceptionally, but just in case we
                            //notify the kill switch to stop the stream.
                            killSwitch.abort(cause);
                        }
                    }
                });

        return new ApplyingJob() {
            @Override
            public CompletableFuture<Empty> onFinishRaw() {
                return whenComplete;
            }

            @Override
            public CompletableFuture<Empty> cancel() {
                return CompletableFuture.supplyAsync(() -> {
                    killSwitch.abort(new CancellationException());
                    return whenComplete.exceptionally(t -> Empty.getInstance())
                            .join();
                }, executor);
            }
        };
    }

    @Override
    public void close() throws Exception {
        LOGGER.trace("Waiting until actor system terminates");
        Await.result(actorSystem.terminate(), Duration.Inf());
        LOGGER.trace("Actor system terminated");
    }

    private Source<OplogBatch, NotUsed> createOplogSource(Creator<OplogFetcher> fetcherCreator) {
        return Source.unfoldResource(
                fetcherCreator,
                this::fetchOplog,
                (fetcher) -> fetcher.close()
        );
    }

    private Optional<OplogBatch> fetchOplog(OplogFetcher fetcher) throws StopReplicationException, RollbackReplicationException {
        OplogBatch batch = fetcher.fetch();
        if (batch.isLastOne()) {
            return Optional.empty();
        }
        return Optional.of(batch);
    }

    /**
     * Creates a flow that batches and analyze a input of {@link AnalyzedOplogBatch remote jobs}.
     *
     * This flow tries to accummulate several remote jobs into a bigger one and does not emit until:
     * <ul>
     * <li>A maximum number of operations are batched</li>
     * <li>Or a maximum time has happen since the last emit</li>
     * <li>Or the recived job is not {@link AnalyzedOplogBatch#isReadyForMore()}</li>
     * </ul>
     * @return
     */
    private Flow<OplogBatch, AnalyzedOplogBatch, NotUsed> createBatcherFlow(ApplierContext context) {
        Predicate<OplogBatch> finishBatchPredicate = (OplogBatch job) -> !job.isReadyForMore();
        Supplier<OplogBatch> zeroFun = () -> NotReadyForMoreOplogBatch.getInstance();
        BiFunction<OplogBatch, OplogBatch, OplogBatch> acumFun = (acumJob, newJob) ->
                acumJob.concat(newJob);

        BatchAnalyzer batchAnalyzer = batchAnalyzerFactory.createBatchAnalyzer(context);
        return Flow.of(OplogBatch.class)
                .via(new BatchFlow<>(batchLimits.maxSize, batchLimits.maxPeriod, finishBatchPredicate, zeroFun,
                        acumFun))
                .map(job -> job.getOps().collect(Collectors.toList()))
                .mapConcat(batchAnalyzer::apply);
    }

    private void storeLastAppliedOp(OplogOperation oplogOp) throws OplogManagerPersistException {
        try (WriteOplogTransaction writeTrans = oplogManager.createWriteTransaction()) {
            writeTrans.forceNewValue(oplogOp.getHash(), oplogOp.getOpTime());
        }
    }

    public static class BatchLimits {
        private final int maxSize;
        private final FiniteDuration maxPeriod;

        public BatchLimits(int maxSize, java.time.Duration maxPeriod) {
            this.maxSize = maxSize;
            this.maxPeriod = new FiniteDuration(maxPeriod.toMillis(), TimeUnit.MILLISECONDS);
        }

        public int getMaxSize() {
            return maxSize;
        }

        public FiniteDuration getMaxPeriod() {
            return maxPeriod;
        }
    }

    private static class BatchFlow<E, A> extends GraphStage<FlowShape<E, A>> {

        public final Inlet<E> in = Inlet.create("in");
        public final Outlet<A> out = Outlet.create("out");
        private final int maxBatchSize;
        private final FiniteDuration period;
        private final Predicate<E> predicate;
        private final Supplier<A> zero;
        private final BiFunction<A, E, A> aggregate;
        private final FlowShape<E,A> shape = FlowShape.of(in, out);
        private static final String MY_TIMER_KEY = "key";

        public BatchFlow(int maxBatchSize, FiniteDuration period, Predicate<E> predicate, Supplier<A> zero, BiFunction<A, E, A> aggregate) {
            this.maxBatchSize = maxBatchSize;
            this.period = period;
            this.predicate = predicate;
            this.zero = zero;
            this.aggregate = aggregate;
        }

        @Override
        public FlowShape<E, A> shape() {
            return shape;
        }

        @Override
        public GraphStageLogic createLogic(Attributes inheritedAtts) {
            return new TimerGraphStageLogic(shape) {
                private A acum = zero.get();
                /**
                 * True iff buff is not empty AND (timer fired OR group is full OR predicate is true)
                 */
                private boolean groupClosed = false;
                private boolean groupEmitted = false;
                private boolean finished = false;
                private int iteration = 0;
                {
                    setHandler(in, new AbstractInHandler() {
                        @Override
                        public void onPush() throws Exception {
                            if (!groupClosed) {
                                nextElement(grab(in));
                            }
                        }

                        @Override
                        public void onUpstreamFinish() throws Exception {
                            finished = true;
                            if (groupEmitted) {
                                completeStage();
                            } else {
                                closeGroup();
                            }
                        }
                    });

                    setHandler(out, new AbstractOutHandler() {
                        @Override
                        public void onPull() throws Exception {
                            if (groupClosed) {
                                emitGroup();
                            }
                        }
                    });
                }

                @Override
                public void preStart() {
                    schedulePeriodically(MY_TIMER_KEY, period);
                    pull(in);
                }

                @Override
                public void onTimer(Object timerKey) {
                    assert timerKey.equals(MY_TIMER_KEY);
                    if (iteration > 0) {
                        closeGroup();
                    }
                }

                private void nextElement(E elem) {
                    groupEmitted = false;
                    acum = aggregate.apply(acum, elem);
                    iteration += 1;
                    if (iteration >= maxBatchSize || predicate.test(elem)) {
                        schedulePeriodically(MY_TIMER_KEY, period);
                        closeGroup();
                    } else {
                        pull(in);
                    }
                }

                private void closeGroup() {
                    groupClosed = true;
                    if (isAvailable(out)) {
                        emitGroup();
                    }
                }

                private void emitGroup() {
                    groupEmitted = true;
                    push(out, acum);
                    acum = null;
                    if (!finished) {
                        startNewGroup();
                    } else {
                        completeStage();
                    }
                }

                private void startNewGroup() {
                    acum = zero.get();
                    iteration = 0;
                    groupClosed = false;
                    if (isAvailable(in)) {
                        nextElement(grab(in));
                    } else {
                        if (!hasBeenPulled(in)) {
                            pull(in);
                        }
                    }
                }
            };
        }

    }

}
