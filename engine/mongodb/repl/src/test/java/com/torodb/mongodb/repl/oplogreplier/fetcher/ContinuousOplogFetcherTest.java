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

package com.torodb.mongodb.repl.oplogreplier.fetcher;

import static org.junit.Assert.assertEquals;

import com.eightkdata.mongowp.OpTime;
import com.eightkdata.mongowp.bson.utils.DefaultBsonValues;
import com.eightkdata.mongowp.client.core.MongoConnection;
import com.eightkdata.mongowp.client.core.UnreachableMongoServerException;
import com.eightkdata.mongowp.server.api.oplog.InsertOplogOperation;
import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.eightkdata.mongowp.server.api.oplog.OplogVersion;
import com.google.common.net.HostAndPort;
import com.torodb.core.metrics.DisabledMetricRegistry;
import com.torodb.core.retrier.Retrier;
import com.torodb.core.retrier.SmartRetrier;
import com.torodb.mongodb.repl.OplogReader;
import com.torodb.mongodb.repl.OplogReaderProvider;
import com.torodb.mongodb.repl.ReplMetrics;
import com.torodb.mongodb.repl.SyncSourceProvider;
import com.torodb.mongodb.repl.exceptions.NoSyncSourceFoundException;
import com.torodb.mongodb.repl.oplogreplier.OpTimeFactory;
import com.torodb.mongodb.repl.oplogreplier.OplogBatch;
import com.torodb.mongodb.repl.oplogreplier.RollbackReplicationException;
import com.torodb.mongodb.repl.oplogreplier.StopReplicationException;
import com.torodb.mongodb.repl.oplogreplier.fetcher.ContinuousOplogFetcher.ContinuousOplogFetcherFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.util.*;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 *
 * @author gortiz
 */
public class ContinuousOplogFetcherTest {

  private Supplier<Collection<OplogOperation>> oplogSupplier;
  @Spy
  private MockedOplogReaderProvider oplogReaderProvider = new MockedOplogReaderProvider();
  @Spy
  private SyncSourceProvider syncSourceProvider = new MockedSyncSourceProvider();
  @Spy
  private Retrier retrier = new SmartRetrier(i -> i > 10, i -> i > 10, i -> i > 10, i -> i
      > 10, (a, m) -> (1 + m) * a);
  private ReplMetrics metrics = new ReplMetrics(new DisabledMetricRegistry());
  private final ContinuousOplogFetcherFactory factory = new ContinuousOplogFetcherFactory() {
    @Override
    public ContinuousOplogFetcher createFetcher(long lastFetchedHash, OpTime lastFetchedOptime) {
      return new ContinuousOplogFetcher(oplogReaderProvider, syncSourceProvider, retrier,
          lastFetchedHash, lastFetchedOptime, metrics);
    }
  };
  private static final OpTimeFactory opTimeFactory = new OpTimeFactory();

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    oplogSupplier = null;
  }

  @Test(expected = StopReplicationException.class)
  public void testEmptyOplog() throws Exception {
    List<OplogOperation> oplog = Collections.emptyList();
    oplogSupplier = () -> oplog;

    ContinuousOplogFetcher fetcher = factory.createFetcher(0, OpTime.EPOCH);

    fetcher.fetch();
  }

  @Test(expected = RollbackReplicationException.class)
  public void testOldOplog() throws Exception {
    int oplogSize = 2;

    List<OplogOperation> oplog = createInsertStream(this::createSimpleInsert)
        .limit(oplogSize)
        .collect(Collectors.toList());
    oplogSupplier = () -> oplog;

    OplogOperation lastOp = oplog.get(oplog.size() - 1);
    ContinuousOplogFetcher fetcher = factory.createFetcher(0,
        opTimeFactory.getNextOpTime(lastOp.getOpTime())
    );

    fetcher.fetch();
  }

  @Test
  public void testShortOplog() throws Exception {
    int oplogSize = 2;

    List<OplogOperation> oplog = createInsertStream(this::createSimpleInsert)
        .limit(oplogSize)
        .collect(Collectors.toList());
    oplogSupplier = () -> oplog;

    OplogOperation firstOp = oplog.get(0);
    ContinuousOplogFetcher fetcher = factory.createFetcher(firstOp.getHash(), firstOp.getOpTime());

    List<OplogOperation> recivedOplog = new ArrayList<>(oplogSize);
    OplogBatch batch = null;
    while (batch == null || !(batch.isLastOne() || batch.isReadyForMore())) {
      batch = fetcher.fetch();
      recivedOplog.addAll(batch.getOps());
    }

    assertEquals("Unexpected number of oplog entries fetched: ", oplog.size() - 1, recivedOplog
        .size());

    assertEquals(oplog.subList(1, oplog.size()), recivedOplog);
  }

  @Test
  public void testMediumOplog() throws Exception {
    int oplogSize = 1000;

    List<OplogOperation> oplog = createInsertStream(this::createSimpleInsert)
        .limit(oplogSize)
        .collect(Collectors.toList());
    oplogSupplier = () -> oplog;

    OplogOperation firstOp = oplog.get(0);
    ContinuousOplogFetcher fetcher = factory.createFetcher(firstOp.getHash(), firstOp.getOpTime());

    List<OplogOperation> recivedOplog = new ArrayList<>(oplogSize);
    OplogBatch batch = null;
    while (batch == null || !(batch.isLastOne() || batch.isReadyForMore())) {
      batch = fetcher.fetch();
      recivedOplog.addAll(batch.getOps());
    }

    assertEquals("Unexpected number of oplog entries fetched: ", oplog.size() - 1, recivedOplog
        .size());

    assertEquals(oplog.subList(1, oplog.size()), recivedOplog);
  }

  @Test
  public void testBigOplog() throws Exception {
    int oplogSize = 100000;

    List<OplogOperation> oplog = createInsertStream(this::createSimpleInsert)
        .limit(oplogSize)
        .collect(Collectors.toList());
    oplogSupplier = () -> oplog;

    OplogOperation firstOp = oplog.get(0);
    ContinuousOplogFetcher fetcher = factory.createFetcher(firstOp.getHash(), firstOp.getOpTime());

    List<OplogOperation> recivedOplog = new ArrayList<>(oplogSize);
    OplogBatch batch = null;
    while (batch == null || !(batch.isLastOne() || !batch.isReadyForMore())) {
      batch = fetcher.fetch();
      recivedOplog.addAll(batch.getOps());
    }

    assertEquals("Unexpected number of oplog entries fetched: ", oplog.size() - 1, recivedOplog
        .size());

    assertEquals(oplog.subList(1, oplog.size()), recivedOplog);
  }

  private class MockedOplogReaderProvider implements OplogReaderProvider {

    private OplogReader newReader() {
      return new StaticOplogReader(oplogSupplier.get());
    }

    @Override
    public OplogReader newReader(HostAndPort syncSource) throws NoSyncSourceFoundException,
        UnreachableMongoServerException {
      return new StaticOplogReader(syncSource, oplogSupplier.get());
    }

    @Override
    public OplogReader newReader(MongoConnection connection) {
      HostAndPort address = connection.getClientOwner().getAddress();
      if (address == null) {
        return new StaticOplogReader(oplogSupplier.get());
      }
      return new StaticOplogReader(address, oplogSupplier.get());
    }

  }

  Stream<OplogOperation> createInsertStream(IntFunction<OplogOperation> intToOplogFun) {
    return IntStream.iterate(0, i -> i + 1)
        .mapToObj(intToOplogFun);
  }

  private OplogOperation createSimpleInsert(int i) {
    return new InsertOplogOperation(
        DefaultBsonValues.newDocument("_id", DefaultBsonValues.newInt(i)),
        "aDb",
        "aCol",
        opTimeFactory.newOpTime(i),
        i,
        OplogVersion.V1,
        false);
  }

  private static class MockedSyncSourceProvider implements SyncSourceProvider {

    private final HostAndPort hostAndPort = HostAndPort.fromParts("localhost", 1);

    @Override
    public HostAndPort newSyncSource() throws NoSyncSourceFoundException {
      return hostAndPort;
    }

    @Override
    public HostAndPort newSyncSource(OpTime lastFetchedOpTime) throws NoSyncSourceFoundException {
      return hostAndPort;
    }

    @Override
    public Optional<HostAndPort> getLastUsedSyncSource() {
      return Optional.of(hostAndPort);
    }

    @Override
    public boolean shouldChangeSyncSource() {
      return false;
    }
  }
}
