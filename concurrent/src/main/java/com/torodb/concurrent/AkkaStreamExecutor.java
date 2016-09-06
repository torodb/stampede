package com.torodb.concurrent;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.dispatch.ExecutionContexts;
import akka.stream.*;
import akka.stream.javadsl.*;
import com.torodb.core.annotations.ParallelLevel;
import com.torodb.core.concurrent.StreamExecutor;
import java.util.concurrent.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.inject.Inject;

/**
 *
 */
public class AkkaStreamExecutor implements StreamExecutor {

    private final int parallelLevel;
    private final Sink<Runnable, CompletionStage<Done>> genericRunnableGraph;
    private final Materializer materializer;
    private final ExecutorService executor;
    private final Consumer<ExecutorService> onClose;

    @Inject
    public AkkaStreamExecutor(@ParallelLevel int parallelLevel, ExecutorService executor,
            Consumer<ExecutorService> onClose) {
        this.parallelLevel = parallelLevel;
        this.executor = executor;
        this.onClose = onClose;

        ActorSystem actorSystem = ActorSystem.create("stream-executor", null, null,
                ExecutionContexts.fromExecutor(executor)
        );
        materializer = ActorMaterializer.create(actorSystem);

        genericRunnableGraph = Flow.fromFunction(
                (Runnable runnable) -> {
                    runnable.run();
                    return NotUsed.getInstance();
                })
                .via(createBalanceGraph(Flow.create()))
                .toMat(Sink.ignore(), Keep.right());
    }

    @Override
    public CompletableFuture<?> executeRunnables(Stream<Runnable> runnables) {
        return Source.fromIterator(() -> runnables.iterator())
                .toMat(genericRunnableGraph, Keep.right())
                .run(materializer)
                .toCompletableFuture();
    }

    @Override
    public <I> CompletableFuture<?> execute(Stream<Callable<I>> callables) {
        Flow<Callable<I>, I, NotUsed> flow = Flow.fromFunction(callable -> callable.call());

        Graph<FlowShape<Callable<I>, I>, NotUsed> balanceGraph = createBalanceGraph(flow);

        return Source.fromIterator(() -> callables.iterator())
                .via(balanceGraph)
                .toMat(Sink.ignore(), Keep.right())
                .run(materializer)
                .toCompletableFuture();
    }

    @Override
    public <I, O> CompletableFuture<O> fold(Stream<Callable<I>> callables, O zero, BiFunction<O, I, O> fun) {

        Flow<Callable<I>, I, NotUsed> flow = Flow.fromFunction(callable -> callable.call());

        Graph<FlowShape<Callable<I>, I>, NotUsed> balanceGraph = createBalanceGraph(flow);

        return Source.fromIterator(() -> callables.iterator())
                .via(balanceGraph)
                .toMat(Sink.fold(zero, (acum, newValue) -> fun.apply(acum, newValue)), Keep.right())
                .run(materializer)
                .toCompletableFuture();
    }

    @Override
    public void close() {
        onClose.accept(executor);
    }

    private <I, O> Graph<FlowShape<I, O>, NotUsed> createBalanceGraph(Flow<I, O, NotUsed> flow) {
        if (parallelLevel == 1) {
            return flow;
        }
        return GraphDSL.create(builder -> {

            UniformFanOutShape<I, I> balance = builder.add(
                    Balance.create(parallelLevel, false)
            );
            UniformFanInShape<O, O> merge = builder.add(
                    Merge.create(parallelLevel, false)
            );

            for (int i = 0; i < parallelLevel; i++) {
                builder.from(balance.out(i))
                        .via(builder.add(
                                flow.async())
                        )
                        .toInlet(merge.in(i));
            }
            return FlowShape.of(balance.in(), merge.out());
        });
    }

}
