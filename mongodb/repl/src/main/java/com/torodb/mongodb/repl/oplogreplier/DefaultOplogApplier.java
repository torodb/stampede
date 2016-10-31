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
import akka.stream.*;
import akka.stream.javadsl.*;
import akka.stream.stage.*;
import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.google.common.base.Supplier;
import com.torodb.core.Shutdowner;
import com.torodb.core.concurrent.ConcurrentToolsFactory;
import com.torodb.mongodb.repl.OplogManager;
import com.torodb.mongodb.repl.OplogManager.OplogManagerPersistException;
import com.torodb.mongodb.repl.OplogManager.WriteOplogTransaction;
import com.torodb.mongodb.repl.oplogreplier.batch.AnalyzedOplogBatch;
import com.torodb.mongodb.repl.oplogreplier.batch.AnalyzedOplogBatchExecutor;
import com.torodb.mongodb.repl.oplogreplier.batch.BatchAnalyzer;
import com.torodb.mongodb.repl.oplogreplier.batch.BatchAnalyzer.BatchAnalyzerFactory;
import com.torodb.mongodb.repl.oplogreplier.fetcher.OplogFetcher;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;
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
    private final OplogApplierMetrics metrics;

    @Inject
    public DefaultOplogApplier(BatchLimits batchLimits, OplogManager oplogManager,
            AnalyzedOplogBatchExecutor batchExecutor, BatchAnalyzerFactory batchAnalyzerFactory,
            ConcurrentToolsFactory concurrentToolsFactory, Shutdowner shutdowner,
            OplogApplierMetrics metrics) {
        this.batchExecutor = batchExecutor;
        this.batchLimits = batchLimits;
        this.oplogManager = oplogManager;
        this.batchAnalyzerFactory = batchAnalyzerFactory;
        this.actorSystem = ActorSystem.create("oplog-applier", null, null,
                ExecutionContexts.fromExecutor(
                        concurrentToolsFactory.createExecutorServiceWithMaxThreads(
                                "oplog-applier", 3)
                )
        );
        this.metrics = metrics;
        shutdowner.addCloseShutdownListener(this);
    }

    @Override
    public ApplyingJob apply(OplogFetcher fetcher, ApplierContext applierContext) {

        Materializer materializer = ActorMaterializer.create(actorSystem);

        RunnableGraph<Pair<UniqueKillSwitch, CompletionStage<Done>>> graph = createOplogSource(fetcher)
                .async()
                .via(createBatcherFlow(applierContext))
                .viaMat(KillSwitches.single(), Keep.right())
                .async()
                .map(analyzedElem -> {
                    for (AnalyzedOplogBatch analyzedOplogBatch : analyzedElem.analyzedBatch) {
                        batchExecutor.apply(analyzedOplogBatch, applierContext);
                    }
                    return analyzedElem;
                })
                .map(this::metricExecution)
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
                            killSwitch.shutdown();
                        } else { //in this case the exception should came from the stream
                            LOGGER.error("Oplog replication stream finished exceptionally", cause);
                            //the stream should be finished exceptionally, but just in case we
                            //notify the kill switch to stop the stream.
                            killSwitch.shutdown();
                        }
                    }
                });

        return new DefaultApplyingJob(killSwitch, whenComplete);
    }

    private static class DefaultApplyingJob extends AbstractApplyingJob {
        private final KillSwitch killSwitch;

        public DefaultApplyingJob(KillSwitch killSwitch,
                CompletableFuture<Empty> onFinish) {
            super(onFinish);
            this.killSwitch = killSwitch;
        }
        
        @Override
        public void cancel() {
            killSwitch.shutdown();
        }
    }

    @Override
    public void close() throws Exception {
        LOGGER.trace("Waiting until actor system terminates");
        Await.result(actorSystem.terminate(), Duration.Inf());
        LOGGER.trace("Actor system terminated");
    }

    private Source<OplogBatch, NotUsed> createOplogSource(OplogFetcher fetcher) {
        return Source.unfold(fetcher, f -> {
            OplogBatch batch = f.fetch();
            if (batch.isLastOne()) {
                return Optional.empty();
            }
            return Optional.of(new Pair<>(f, batch));
        });
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
    private Flow<OplogBatch, AnalyzedStreamElement, NotUsed> createBatcherFlow(ApplierContext context) {
        Predicate<OplogBatch> finishBatchPredicate = (OplogBatch rawBatch) -> !rawBatch.isReadyForMore();
        ToIntFunction<OplogBatch> costFunction = (rawBatch) -> rawBatch.count();

        Supplier<RawStreamElement> zeroFun = () -> RawStreamElement.INITIAL_ELEMENT;
        BiFunction<RawStreamElement, OplogBatch, RawStreamElement> acumFun = (streamElem, newBatch) ->
                streamElem.concat(newBatch);

        BatchAnalyzer batchAnalyzer = batchAnalyzerFactory.createBatchAnalyzer(context);
        return Flow.of(OplogBatch.class)
                .via(new BatchFlow<>(batchLimits.maxSize, batchLimits.maxPeriod,
                        finishBatchPredicate, costFunction, zeroFun, acumFun))
                .filter(rawElem -> rawElem.rawBatch != null && !rawElem.rawBatch.isEmpty())
                .map(rawElem -> {
                    List<OplogOperation> rawOps = rawElem.rawBatch.getOps();
                    List<AnalyzedOplogBatch> analyzed = batchAnalyzer.apply(rawOps);
                    return new AnalyzedStreamElement(rawElem, analyzed);
                });
    }

    private AnalyzedStreamElement storeLastAppliedOp(AnalyzedStreamElement streamElement) throws OplogManagerPersistException {
        assert !streamElement.rawBatch.isEmpty();
        OplogOperation lastOp = streamElement.rawBatch.getLastOperation();
        try (WriteOplogTransaction writeTrans = oplogManager.createWriteTransaction()) {
            writeTrans.forceNewValue(lastOp.getHash(), lastOp.getOpTime());
        }
        return streamElement;
    }

    private AnalyzedStreamElement metricExecution(AnalyzedStreamElement streamElement) {
        long timestamp = System.currentTimeMillis();
        long batchExecutionMillis = timestamp - streamElement.startFetchTimestamp;

        int rawBatchSize = streamElement.rawBatch.count();
        metrics.getBatchSize().update(rawBatchSize);
        metrics.getApplied().mark(rawBatchSize);

        metricOpsExecutionDelay(rawBatchSize, batchExecutionMillis);

        return streamElement;
    }

    private void metricOpsExecutionDelay(int rawBatchSize, long batchExecutionMillis) {
        if (rawBatchSize < 1) {
            return ;
        }
        if (batchExecutionMillis <= 0) {
            LOGGER.debug("Unexpected time execution: {}" + batchExecutionMillis);
        }
        metrics.getMaxDelay().update(batchExecutionMillis);
        metrics.getApplicationCost().update((1000l * batchExecutionMillis) / rawBatchSize);
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

    private static class RawStreamElement {
        private static final RawStreamElement INITIAL_ELEMENT = new RawStreamElement(null, 0);
        private final OplogBatch rawBatch;
        private final long startFetchTimestamp;
        
        public RawStreamElement(OplogBatch rawBatch, long startFetchTimestamp) {
            this.rawBatch = rawBatch;
            this.startFetchTimestamp = startFetchTimestamp;
        }

        private RawStreamElement concat(OplogBatch newBatch) {
            OplogBatch newRawBatch;
            long newStartFetchTimestamp;
            if (this == INITIAL_ELEMENT) {
                newRawBatch = newBatch;
                newStartFetchTimestamp = System.currentTimeMillis();
            } else {
                newRawBatch = rawBatch.concat(newBatch);
                newStartFetchTimestamp = startFetchTimestamp;
            }
            return new RawStreamElement(newRawBatch, newStartFetchTimestamp);
        }
    }

    private static class AnalyzedStreamElement {
        private final OplogBatch rawBatch;
        private final long startFetchTimestamp;
        private final List<AnalyzedOplogBatch> analyzedBatch;

        AnalyzedStreamElement(RawStreamElement rawStreamElement, List<AnalyzedOplogBatch> analyzedBatches) {
            this.rawBatch = rawStreamElement.rawBatch;
            this.startFetchTimestamp = rawStreamElement.startFetchTimestamp;
            this.analyzedBatch = analyzedBatches;
        }

    }

    private static class BatchFlow<E, A> extends GraphStage<FlowShape<E, A>> {

        public final Inlet<E> in = Inlet.create("in");
        public final Outlet<A> out = Outlet.create("out");
        private final int maxBatchSize;
        private final FiniteDuration period;
        private final Predicate<E> predicate;
        private final ToIntFunction<E> costFunction;
        private final Supplier<A> zero;
        private final BiFunction<A, E, A> aggregate;
        private final FlowShape<E,A> shape = FlowShape.of(in, out);
        private static final String MY_TIMER_KEY = "key";

        public BatchFlow(int maxBatchSize, FiniteDuration period, Predicate<E> predicate,
                ToIntFunction<E> costFunction, Supplier<A> zero, BiFunction<A, E, A> aggregate) {
            this.maxBatchSize = maxBatchSize;
            this.period = period;
            this.predicate = predicate;
            this.costFunction = costFunction;
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
                    iteration += costFunction.applyAsInt(elem);
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
