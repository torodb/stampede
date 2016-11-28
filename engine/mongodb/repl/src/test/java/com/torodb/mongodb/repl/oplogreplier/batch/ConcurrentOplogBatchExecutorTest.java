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

package com.torodb.mongodb.repl.oplogreplier.batch;

import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.*;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.google.common.collect.Lists;
import com.torodb.core.concurrent.ConcurrentToolsFactory;
import com.torodb.core.concurrent.StreamExecutor;
import com.torodb.core.retrier.Retrier;
import com.torodb.kvdocument.values.KvInteger;
import com.torodb.mongodb.core.MongodConnection;
import com.torodb.mongodb.core.MongodServer;
import com.torodb.mongodb.core.WriteMongodTransaction;
import com.torodb.mongodb.repl.oplogreplier.ApplierContext;
import com.torodb.mongodb.repl.oplogreplier.OplogOperationApplier;
import com.torodb.mongodb.repl.oplogreplier.analyzed.AnalyzedOp;
import com.torodb.mongodb.repl.oplogreplier.analyzed.DebuggingAnalyzedOp;
import com.torodb.mongodb.repl.oplogreplier.batch.ConcurrentOplogBatchExecutor.ConcurrentOplogBatchExecutorMetrics;
import com.torodb.mongodb.repl.oplogreplier.batch.ConcurrentOplogBatchExecutor.SubBatchHeuristic;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 *
 * @author gortiz
 */
public class ConcurrentOplogBatchExecutorTest {

  @Mock
  private ConcurrentOplogBatchExecutorMetrics metrics;
  @Mock
  private StreamExecutor streamExecutor;
  private ConcurrentToolsFactory concurrentToolsFactory;
  @Mock
  private SubBatchHeuristic subBatchHeuristic;
  @Mock
  private OplogOperationApplier applier;
  @Mock
  private MongodServer server;
  @Mock
  private Retrier retrier;
  @Mock
  private NamespaceJobExecutor namespaceJobExecutor;
  @Mock
  private MongodConnection conn;
  @Mock
  private WriteMongodTransaction writeTrans;
  private ConcurrentOplogBatchExecutor executor;
  private int idFactory;

  @Before
  public void setUp() {
    idFactory = 0;
    MockitoAnnotations.initMocks(this);
    concurrentToolsFactory = mock(ConcurrentToolsFactory.class);
    when(concurrentToolsFactory.getDefaultMaxThreads()).thenReturn(4);
    when(concurrentToolsFactory.createStreamExecutor(any(String.class), any(Boolean.class)))
        .thenReturn(streamExecutor);
    executor = spy(
        new ConcurrentOplogBatchExecutor(applier, server, retrier, concurrentToolsFactory,
            namespaceJobExecutor, metrics, subBatchHeuristic));
  }

  @Test
  public void testExecute() throws Exception {
    int batchSize = 100;
    int opsPerJob = 20;
    int subBatchSize = 11;
    int subBatchesPerJob = opsPerJob / subBatchSize + (opsPerJob % subBatchSize != 0 ? 1 : 0);
    int expectedSize = batchSize * subBatchesPerJob;
    //GIVEN
    CudAnalyzedOplogBatch batch = mock(CudAnalyzedOplogBatch.class);
    List<NamespaceJob> jobs = new ArrayList<>();
    for (int i = 0; i < batchSize; i++) {
      jobs.add(
          new NamespaceJob(
              "db",
              "col",
              Lists.newArrayList(
                  Stream.iterate(createAnalyzedOp(null), this::createAnalyzedOp)
                      .limit(opsPerJob)
                      .iterator()
              )
          )
      );
    }
    AtomicInteger callablesCounter = new AtomicInteger(0);

    ApplierContext context = new ApplierContext.Builder()
        .setReapplying(true)
        .setUpdatesAsUpserts(true)
        .build();
    Histogram mockHistogram = mock(Histogram.class);
    Meter mockMeter = mock(Meter.class);

    given(batch.streamNamespaceJobs()).willReturn(jobs.stream());
    given(subBatchHeuristic.getSubBatchSize(any())).willReturn(subBatchSize);
    given(metrics.getSubBatchSizeHistogram()).willReturn(mockHistogram);
    given(metrics.getSubBatchSizeMeter()).willReturn(mockMeter);
    given(streamExecutor.execute(any()))
        .willAnswer(new Answer<CompletableFuture<?>>() {
          @Override
          public CompletableFuture<?> answer(InvocationOnMock invocation) throws Throwable {
            CompletableFuture<Object> completableFuture = new CompletableFuture<>();
            completableFuture.complete(new Object());

            Stream<Callable<?>> callables = invocation.getArgument(0);
            callablesCounter.addAndGet((int) callables.count());

            return completableFuture;
          }
        });

    //WHEN
    executor.execute(batch, context);

    //THEN
    then(mockHistogram).should().update(expectedSize);
    then(mockMeter).should().mark(expectedSize);
    assertEquals(expectedSize, callablesCounter.get());
  }

  private AnalyzedOp createAnalyzedOp(AnalyzedOp ignored) {
    int id = idFactory++;
    return new DebuggingAnalyzedOp(KvInteger.of(id));
  }

}
