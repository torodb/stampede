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

import akka.Done;
import akka.NotUsed;
import akka.stream.ActorMaterializer;
import akka.stream.FlowShape;
import akka.stream.Graph;
import akka.stream.Materializer;
import akka.stream.UniformFanInShape;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.Balance;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Merge;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.google.common.base.Preconditions;
import com.torodb.core.annotations.ParallelLevel;
import com.torodb.core.concurrent.StreamExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import javax.inject.Inject;

/**
 *
 */
public class AkkaStreamExecutor extends ActorSystemTorodbService
    implements StreamExecutor {

  private static final Logger LOGGER =
      LogManager.getLogger(AkkaStreamExecutor.class);
  private final int parallelLevel;
  private final Sink<Runnable, CompletionStage<Done>> genericRunnableGraph;
  private Materializer materializer;

  @Inject
  public AkkaStreamExecutor(ThreadFactory threadFactory,
      @ParallelLevel int parallelLevel, ExecutorService executor,
      String prefix) {
    super(threadFactory, () -> executor, prefix);
    this.parallelLevel = parallelLevel;

    genericRunnableGraph = Flow.fromFunction(
        (Runnable runnable) -> {
          runnable.run();
          return NotUsed.getInstance();
        })
        .via(createBalanceGraph(Flow.create()))
        .toMat(Sink.ignore(), Keep.right());
  }

  @Override
  protected Logger getLogger() {
    return LOGGER;
  }

  @Override
  protected void startUp() throws Exception {
    super.startUp();
    materializer = ActorMaterializer.create(getActorSystem());
  }

  @Override
  public CompletableFuture<?> executeRunnables(Stream<Runnable> runnables) {
    Preconditions.checkState(isRunning(), "This service is not running");
    return Source.fromIterator(() -> runnables.iterator())
        .toMat(genericRunnableGraph, Keep.right())
        .run(materializer)
        .toCompletableFuture();
  }

  @Override
  public <I> CompletableFuture<?> execute(Stream<Callable<I>> callables) {
    Preconditions.checkState(isRunning(), "This service is not running");
    Flow<Callable<I>, I, NotUsed> flow = Flow.fromFunction(callable -> callable.call());

    Graph<FlowShape<Callable<I>, I>, NotUsed> balanceGraph = createBalanceGraph(flow);

    return Source.fromIterator(() -> callables.iterator())
        .via(balanceGraph)
        .toMat(Sink.ignore(), Keep.right())
        .run(materializer)
        .toCompletableFuture();
  }

  @Override
  public <I, O> CompletableFuture<O> fold(Stream<Callable<I>> callables, O zero,
      BiFunction<O, I, O> fun) {
    Preconditions.checkState(isRunning(), "This service is not running");
    Flow<Callable<I>, I, NotUsed> flow = Flow.fromFunction(callable -> callable.call());

    Graph<FlowShape<Callable<I>, I>, NotUsed> balanceGraph = createBalanceGraph(flow);

    return Source.fromIterator(() -> callables.iterator())
        .via(balanceGraph)
        .toMat(Sink.fold(zero, (acum, newValue) -> fun.apply(acum, newValue)), Keep.right())
        .run(materializer)
        .toCompletableFuture();
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
