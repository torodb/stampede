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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.google.common.collect.Lists;
import com.torodb.core.exceptions.user.DatabaseNotFoundException;
import com.torodb.core.retrier.AlwaysRetryRetrier;
import com.torodb.core.retrier.NeverRetryRetrier;
import com.torodb.core.retrier.Retrier;
import com.torodb.core.retrier.RetrierAbortException;
import com.torodb.core.retrier.RetrierGiveUpException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.mongodb.core.ExclusiveWriteMongodTransaction;
import com.torodb.mongodb.core.MongodConnection;
import com.torodb.mongodb.core.MongodServer;
import com.torodb.mongodb.repl.oplogreplier.ApplierContext;
import com.torodb.mongodb.repl.oplogreplier.OplogOperationApplier;
import com.torodb.mongodb.repl.oplogreplier.OplogOperationApplier.OplogApplyingException;
import com.torodb.mongodb.repl.oplogreplier.batch.AnalyzedOplogBatchExecutor.AnalyzedOplogBatchExecutorMetrics;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.stream.Stream;

public class SimpleAnalyzedOplogBatchExecutorTest {

  @Mock
  private AnalyzedOplogBatchExecutorMetrics metrics;
  @Mock
  private OplogOperationApplier applier;
  @Mock
  private MongodServer server;
  private Retrier retrier;
  @Mock
  private NamespaceJobExecutor namespaceJobExecutor;
  @Mock
  private MongodConnection conn;
  @Mock
  private ExclusiveWriteMongodTransaction writeTrans;
  private SimpleAnalyzedOplogBatchExecutor executor;
  private SimpleAnalyzedOplogBatchExecutor actualExecutor;

  @Before
  public void setUp() {
    this.retrier = spy(NeverRetryRetrier.getInstance());
    MockitoAnnotations.initMocks(this);

    actualExecutor = new SimpleAnalyzedOplogBatchExecutor(metrics, applier,
        server, retrier, namespaceJobExecutor);

    executor = spy(actualExecutor);

    given(server.openConnection()).willReturn(conn);
    given(conn.openExclusiveWriteTransaction()).willReturn(writeTrans);
    given(conn.openWriteTransaction()).willReturn(writeTrans);

    given(metrics.getCudBatchSize()).willReturn(mock(Histogram.class));
    given(metrics.getCudBatchTimer()).willReturn(mock(Timer.class));
    given(metrics.getNamespaceBatchTimer()).willReturn(mock(Timer.class));
    given(metrics.getCudBatchSize()).willReturn(mock(Histogram.class));

    actualExecutor.startAsync();
    actualExecutor.awaitRunning();
  }

  @After
  public void tearDown() {
    actualExecutor.stopAsync();
    actualExecutor.awaitTerminated();
  }

  @Test
  public void testExecute_OplogOperation() throws Exception {
    //GIVEN
    OplogOperation op = mock(OplogOperation.class);
    ApplierContext applierContext = new ApplierContext.Builder()
        .setReapplying(true)
        .setUpdatesAsUpserts(true)
        .build();

    //WHEN
    executor.execute(op, applierContext);

    //THEN
    then(server).should().openConnection();
    then(conn).should().close();
    then(conn).should().openExclusiveWriteTransaction();
    then(writeTrans).should().close();
    then(applier).should().apply(op, writeTrans, applierContext);
  }

  @Test
  public void testExecute_CudAnalyzedOplogBatch() throws Exception {
    //GIVEN
    CudAnalyzedOplogBatch cudBatch = mock(CudAnalyzedOplogBatch.class);
    ApplierContext applierContext = new ApplierContext.Builder()
        .setReapplying(true)
        .setUpdatesAsUpserts(true)
        .build();

    NamespaceJob job1 = mock(NamespaceJob.class);
    NamespaceJob job2 = mock(NamespaceJob.class);
    NamespaceJob job3 = mock(NamespaceJob.class);

    doNothing().when(executor).execute(any(), any(), any());
    given(cudBatch.streamNamespaceJobs())
        .willReturn(Stream.of(job1, job2, job3));

    //WHEN
    executor.execute(cudBatch, applierContext);

    //THEN
    then(server).should().openConnection();
    then(conn).should().close();
    then(executor).should().execute(job1, applierContext, conn);
    then(executor).should().execute(job2, applierContext, conn);
    then(executor).should().execute(job3, applierContext, conn);
  }

  @Test
  public void testExecute_NamespaceJob() throws Exception {
    //GIVEN
    ApplierContext applierContext = new ApplierContext.Builder()
        .setReapplying(true)
        .setUpdatesAsUpserts(true)
        .build();
    Context context = mock(Context.class);
    NamespaceJob job = mock(NamespaceJob.class);
    given(metrics.getNamespaceBatchTimer().time()).willReturn(context);

    //WHEN
    executor.execute(job, applierContext, conn);

    //THEN
    then(metrics).should(atLeastOnce()).getNamespaceBatchTimer();
    then(metrics.getNamespaceBatchTimer()).should().time();
    then(context).should().close();
    //TODO: This might be changed once the backend throws UniqueIndexViolation
    then(namespaceJobExecutor).should().apply(eq(job), eq(writeTrans), eq(applierContext), any(
        Boolean.class));
  }

  @Test
  public void testVisit_SingleOp_Success() throws Exception {
    //GIVEN
    OplogOperation operation = mock(OplogOperation.class);
    SingleOpAnalyzedOplogBatch batch = new SingleOpAnalyzedOplogBatch(operation);
    ApplierContext applierContext = new ApplierContext.Builder()
        .setReapplying(true)
        .setUpdatesAsUpserts(true)
        .build();

    Timer timer = mock(Timer.class);
    Context context = mock(Context.class);
    given(metrics.getSingleOpTimer(operation)).willReturn(timer);
    given(timer.time()).willReturn(context);
    doNothing().when(executor).execute(any(OplogOperation.class), any());

    //WHEN
    OplogOperation result = executor.visit(batch, applierContext);

    //THEN
    then(metrics).should().getSingleOpTimer(operation);
    then(timer).should().time();
    then(context).should().close();
    then(executor).should(times(1)).execute(operation, applierContext);
    assertEquals(operation, result);
  }

  @Test
  public void testVisit_SingleOp_OplogApplyingEx() throws Exception {
    //GIVEN
    OplogOperation operation = mock(OplogOperation.class);
    SingleOpAnalyzedOplogBatch batch = new SingleOpAnalyzedOplogBatch(operation);
    ApplierContext applierContext = new ApplierContext.Builder()
        .setReapplying(true)
        .setUpdatesAsUpserts(true)
        .build();

    Timer timer = mock(Timer.class);
    Context context = mock(Context.class);
    given(metrics.getSingleOpTimer(operation)).willReturn(timer);
    given(timer.time()).willReturn(context);
    doThrow(new OplogApplyingException(new MongoException(ErrorCode.BAD_VALUE)))
        .when(executor)
        .execute(operation, applierContext);

    //WHEN
    try {
      executor.visit(batch, applierContext);
      fail("An exception was expected");
    } catch (RetrierGiveUpException | RetrierAbortException ignore) {
    }

    //THEN
    then(metrics).should().getSingleOpTimer(operation);
    then(timer).should().time();
    then(executor).should(times(1)).execute(operation, applierContext);
  }

  @Test
  public void testVisit_SingleOp_UserEx() throws Exception {
    //GIVEN
    OplogOperation operation = mock(OplogOperation.class);
    SingleOpAnalyzedOplogBatch batch = new SingleOpAnalyzedOplogBatch(operation);
    ApplierContext applierContext = new ApplierContext.Builder()
        .setReapplying(true)
        .setUpdatesAsUpserts(true)
        .build();

    Timer timer = mock(Timer.class);
    Context context = mock(Context.class);
    given(metrics.getSingleOpTimer(operation)).willReturn(timer);
    given(timer.time()).willReturn(context);
    doThrow(new DatabaseNotFoundException("test"))
        .when(executor)
        .execute(operation, applierContext);

    //WHEN
    try {
      executor.visit(batch, applierContext);
      fail("An exception was expected");
    } catch (RetrierGiveUpException | RetrierAbortException ignore) {
    }

    //THEN
    then(metrics).should().getSingleOpTimer(operation);
    then(timer).should().time();
    then(executor).should(times(1)).execute(operation, applierContext);
  }

  /**
   * Test the behaviour of the method
   * {@link SimpleAnalyzedOplogBatchExecutor#visit(com.torodb.mongodb.repl.oplogreplier.batch.SingleOpAnalyzedOplogBatch, com.torodb.mongodb.repl.oplogreplier.ApplierContext) that visits a single op}
   * when
   * {@link SimpleAnalyzedOplogBatchExecutor#execute(com.eightkdata.mongowp.server.api.oplog.OplogOperation, com.torodb.mongodb.repl.oplogreplier.ApplierContext) the execution}
   * fails until the given attempt.
   *
   *
   * @param myRetrier
   * @param atteptsToSucceed
   * @return true if the execution finishes or false if it throw an exception.
   * @throws Exception
   */
  private boolean testVisit_SingleOp_NotRepyingRollback(Retrier myRetrier, int atteptsToSucceed)
      throws Exception {
    //GIVEN
    OplogOperation operation = mock(OplogOperation.class);
    SingleOpAnalyzedOplogBatch batch = new SingleOpAnalyzedOplogBatch(operation);
    ApplierContext applierContext = new ApplierContext.Builder()
        .setReapplying(true)
        .setUpdatesAsUpserts(false)
        .build();
    executor = spy(new SimpleAnalyzedOplogBatchExecutor(metrics, applier, server, myRetrier,
        namespaceJobExecutor));

    Timer timer = mock(Timer.class);
    Context context = mock(Context.class);
    given(metrics.getSingleOpTimer(operation)).willReturn(timer);
    given(timer.time()).willReturn(context);
    doAnswer(new Answer() {
      int attempts = 0;

      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        try {
          ApplierContext context = invocation.getArgument(1);
          if (attempts == 0) {
            assert !context.treatUpdateAsUpsert() :
                "on this test, first attept should be not trying updates as upserts";
            throw new RollbackException("Forcing a rollback on the first attempt");
          }
          assert context.treatUpdateAsUpsert() : "on this test, only the first attept should be "
              + "not trying updates as upserts, but " + attempts
              + " is not trying updates as upserts";
          if (attempts < (atteptsToSucceed - 1)) {
            throw new RollbackException("forcing a rollback on the " + attempts + "th attempt");
          }
          return null;
        } finally {
          attempts++;
        }
      }
    }).when(executor)
        .execute(eq(operation), any());

    try {
      //WHEN
      OplogOperation result = executor.visit(batch, applierContext);

      //THEN
      then(executor).should(times(atteptsToSucceed)).execute(eq(operation), any());
      assertEquals(operation, result);
      return true;
    } catch (RetrierGiveUpException ignore) {
      return false;
    } finally {
      then(metrics).should().getSingleOpTimer(operation);
      then(timer).should().time();
    }
  }

  @Test
  public void testVisit_SingleOp_NotReplyingRollback_NotGivingUp() throws Exception {
    boolean succees = testVisit_SingleOp_NotRepyingRollback(AlwaysRetryRetrier.getInstance(), 10);
    assertTrue("It was expected that this execution success!", succees);
  }

  @Test
  public void testVisit_SingleOp_NotReplyingRollback_GivingUp() throws Exception {
    boolean succees = testVisit_SingleOp_NotRepyingRollback(NeverRetryRetrier.getInstance(), 10);
    assertFalse("It was expected that this execution fails!", succees);
  }

  @Test
  public void testVisit_CudAnalyzedOplog_Success() throws Exception {
    //GIVEN
    OplogOperation lastOp = mock(OplogOperation.class);
    CudAnalyzedOplogBatch batch = mock(CudAnalyzedOplogBatch.class);
    ApplierContext applierContext = new ApplierContext.Builder()
        .setReapplying(true)
        .setUpdatesAsUpserts(true)
        .build();
    given(batch.getOriginalBatch()).willReturn(Lists.newArrayList(
        mock(OplogOperation.class),
        mock(OplogOperation.class),
        mock(OplogOperation.class),
        lastOp
    ));

    Timer timer = mock(Timer.class);
    Context context = mock(Context.class);
    given(metrics.getCudBatchTimer()).willReturn(timer);
    given(timer.time()).willReturn(context);
    doNothing().when(executor).execute(eq(batch), any());

    //WHEN
    OplogOperation result = executor.visit(batch, applierContext);

    //THEN
    then(metrics).should().getCudBatchTimer();
    then(timer).should().time();
    then(context).should().close();
    then(executor).should(times(1)).execute(batch, applierContext);
    assertEquals(lastOp, result);
  }

  @Test
  public void testVisit_CudAnalyzedOplog_UserEx() throws Exception {
    //GIVEN
    OplogOperation lastOp = mock(OplogOperation.class);
    CudAnalyzedOplogBatch batch = mock(CudAnalyzedOplogBatch.class);
    ApplierContext applierContext = new ApplierContext.Builder()
        .setReapplying(true)
        .setUpdatesAsUpserts(true)
        .build();
    given(batch.getOriginalBatch()).willReturn(Lists.newArrayList(
        mock(OplogOperation.class),
        mock(OplogOperation.class),
        mock(OplogOperation.class),
        lastOp
    ));

    Timer timer = mock(Timer.class);
    Context context = mock(Context.class);
    given(metrics.getCudBatchTimer()).willReturn(timer);
    given(timer.time()).willReturn(context);
    doThrow(new DatabaseNotFoundException("test"))
        .when(executor)
        .execute(eq(batch), any());

    //WHEN
    try {
      executor.visit(batch, applierContext);
      fail("An exception was expected");
    } catch (RetrierGiveUpException | RetrierAbortException ignore) {
    }

    //THEN
    then(metrics).should().getCudBatchTimer();
    then(timer).should().time();
    then(executor).should(times(1)).execute(batch, applierContext);
  }

  /**
   * Test the behaviour of the method
   * {@link SimpleAnalyzedOplogBatchExecutor#visit(com.torodb.mongodb.repl.oplogreplier.batch.CudAnalyzedOplogBatch, com.torodb.mongodb.repl.oplogreplier.ApplierContext)  that visits a cud batch}
   * when
   * {@link SimpleAnalyzedOplogBatchExecutor#execute(com.torodb.mongodb.repl.oplogreplier.batch.CudAnalyzedOplogBatch, com.torodb.mongodb.repl.oplogreplier.ApplierContext) the execution}
   * fails until the given attempt.
   *
   *
   * @param myRetrier
   * @param atteptsToSucceed
   * @return true if the execution finishes or false if it throw an exception.
   * @throws Exception
   */
  private boolean testVisit_CudAnalyzedOplog_NotRepyingRollback(Retrier myRetrier,
      int atteptsToSucceed) throws Exception {
    //GIVEN
    OplogOperation lastOp = mock(OplogOperation.class);
    CudAnalyzedOplogBatch batch = mock(CudAnalyzedOplogBatch.class);
    ApplierContext applierContext = new ApplierContext.Builder()
        .setReapplying(true)
        .setUpdatesAsUpserts(false)
        .build();
    given(batch.getOriginalBatch()).willReturn(Lists.newArrayList(
        mock(OplogOperation.class),
        mock(OplogOperation.class),
        mock(OplogOperation.class),
        lastOp
    ));
    executor = spy(new SimpleAnalyzedOplogBatchExecutor(metrics, applier, server, myRetrier,
        namespaceJobExecutor));

    Timer timer = mock(Timer.class);
    Context context = mock(Context.class);
    given(metrics.getCudBatchTimer()).willReturn(timer);
    given(timer.time()).willReturn(context);
    doAnswer(new Answer() {
      int attempts = 0;

      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        try {
          ApplierContext context = invocation.getArgument(1);
          if (attempts == 0) {
            assert !context.treatUpdateAsUpsert() :
                "on this test, first attept should be not trying updates as upserts";
            throw new RollbackException("Forcing a rollback on the first attempt");
          }
          assert context.treatUpdateAsUpsert() : "on this test, only the first attept should be "
              + "not trying updates as upserts, but " + attempts
              + " is not trying updates as upserts";
          if (attempts < (atteptsToSucceed - 1)) {
            throw new RollbackException("forcing a rollback on the " + attempts + "th attempt");
          }
          return null;
        } finally {
          attempts++;
        }
      }
    }).when(executor)
        .execute(eq(batch), any());

    boolean success;

    try {
      //WHEN
      OplogOperation result = executor.visit(batch, applierContext);

      //THEN
      then(executor).should(times(atteptsToSucceed)).execute(eq(batch), any());
      assertEquals(lastOp, result);
      success = true;
    } catch (RetrierGiveUpException ignore) {
      success = false;
    }
    then(metrics.getCudBatchSize()).should().update(batch.getOriginalBatch().size());
    then(metrics).should().getCudBatchTimer();
    then(metrics.getCudBatchTimer()).should().time();

    return success;
  }

  @Test
  public void testVisit_CudAnalyzedOplog_NotReplyingRollback_NotGivingUp() throws Exception {
    boolean succees =
        testVisit_CudAnalyzedOplog_NotRepyingRollback(AlwaysRetryRetrier.getInstance(), 10);
    assertTrue("It was expected that this execution success!", succees);
  }

  @Test
  public void testVisit_CudAnalyzedOplog_NotReplyingRollback_GivingUp() throws Exception {
    boolean succees = testVisit_CudAnalyzedOplog_NotRepyingRollback(NeverRetryRetrier.getInstance(),
        10);
    assertFalse("It was expected that this execution fails!", succees);
  }

}
