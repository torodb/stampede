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

package com.torodb.metainfo.cache.mvcc;

import static com.google.common.truth.Truth.assertThat;

import com.torodb.common.util.Sequencer;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MetainfoRepository.MergerStage;
import com.torodb.core.transaction.metainf.MetainfoRepository.SnapshotStage;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import com.torodb.core.transaction.metainf.UnmergeableException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 *
 * @author gortiz
 */
public class MvccMetainfoRepositoryTest {

  private MvccMetainfoRepository repository;

  private final String dbName = "dbName";
  private final String dbId = "dbId";
  private final String colName = "colName";
  private final String colId = "colId";

  private static final long MILLIS_TO_WAIT = 1_000;

  public MvccMetainfoRepositoryTest() {
  }

  @Before
  public void setUp() throws Exception {
    repository = new MvccMetainfoRepository();
  }

  @Test
  public void testSingleThread() throws UnmergeableException {
    MutableMetaSnapshot mutableSnapshot;

    try (SnapshotStage snapshotStage = repository.startSnapshotStage()) {
      mutableSnapshot = snapshotStage.createMutableSnapshot();
    }

    mutableSnapshot.addMetaDatabase(dbName, dbId)
        .addMetaCollection(colName, colId);

    Assert.assertNotNull(mutableSnapshot.getMetaDatabaseByName(dbName));
    Assert.assertNotNull(mutableSnapshot.getMetaDatabaseByName(dbName)
        .getMetaCollectionByIdentifier(colId));

    try (MergerStage mergeStage = repository.startMerge(mutableSnapshot)) {
      mergeStage.commit();
    }

    ImmutableMetaSnapshot immutableSnapshot;
    try (SnapshotStage snapshotStage = repository.startSnapshotStage()) {
      immutableSnapshot = snapshotStage.createImmutableSnapshot();
    }

    assertThat(immutableSnapshot.getMetaDatabaseByName(dbName))
        .named("the database by name")
        .isNotNull();
    Assert.assertNotNull(immutableSnapshot.getMetaDatabaseByName(dbName));
    Assert.assertNotNull(immutableSnapshot.getMetaDatabaseByName(dbName)
        .getMetaCollectionByIdentifier(colId));
  }

  /**
   * Tests if changes on a thread are seen by another thread after a merge phase.
   *
   * @throws Throwable
   */
  @Test
  public void testReadCommited() throws Throwable {
    final Sequencer<ReaderRunnable.ReaderPhase> readerSequencer = new Sequencer<>(
        ReaderRunnable.ReaderPhase.class);
    final Sequencer<WriterRunnable.WriterPhase> writerSequencer = new Sequencer<>(
        WriterRunnable.WriterPhase.class);

    writerSequencer.notify(WriterRunnable.WriterPhase.values());

    WriterRunnable writerRunnable = new WriterRunnable(repository, writerSequencer) {
      @Override
      protected void postSnapshot(MutableMetaSnapshot snapshot) {
        snapshot.addMetaDatabase(dbName, dbId)
            .addMetaCollection(colName, colId);
      }

      @Override
      protected void postMerge(MutableMetaSnapshot snapshot) {
        readerSequencer.notify(ReaderRunnable.ReaderPhase.values());
      }

    };
    ReaderRunnable readerRunnable = new ReaderRunnable(repository, readerSequencer) {
      @Override
      protected void callback(ImmutableMetaSnapshot snapshot) {
        Assert.assertNotNull(snapshot.getMetaDatabaseByName(dbName));
        Assert.assertNotNull(snapshot.getMetaDatabaseByName(dbName).getMetaCollectionByIdentifier(
            colId));
      }
    };

    executeConcurrent(MILLIS_TO_WAIT, readerRunnable, writerRunnable);
  }

  /**
   * Test whether modifications are seen before merge finishes.
   *
   * @throws Throwable
   */
  @Test
  public void testReadUncommited() throws Throwable {
    final Sequencer<ReaderRunnable.ReaderPhase> readerSequencer = new Sequencer<>(
        ReaderRunnable.ReaderPhase.class);
    final Sequencer<WriterRunnable.WriterPhase> writerSequencer = new Sequencer<>(
        WriterRunnable.WriterPhase.class);

    //Writer will wait until PRE_MERGE is sent
    writerSequencer.notify(WriterRunnable.WriterPhase.PRE_SNAPSHOT);
    writerSequencer.notify(WriterRunnable.WriterPhase.POST_SNAPSHOT);
    writerSequencer.notify(WriterRunnable.WriterPhase.POST_MERGE);

    //Reader will wait until writer modifies its snapshot
    WriterRunnable writerRunnable = new WriterRunnable(repository, writerSequencer) {

      @Override
      protected void postSnapshot(MutableMetaSnapshot snapshot) {
        snapshot.addMetaDatabase(dbName, dbId)
            .addMetaCollection(colName, colId);

        readerSequencer.notify(ReaderRunnable.ReaderPhase.PRE_SNAPSHOT);
        readerSequencer.notify(ReaderRunnable.ReaderPhase.PRE_CALLBACK);
      }
    };
    ReaderRunnable readerRunnable = new ReaderRunnable(repository, readerSequencer) {
      @Override
      protected void callback(ImmutableMetaSnapshot snapshot) {
        Assert.assertNull(snapshot.getMetaDatabaseByName(dbName));

        //after reader asserts, writer can merge
        writerSequencer.notify(WriterRunnable.WriterPhase.PRE_MERGE);
      }
    };

    executeConcurrent(MILLIS_TO_WAIT, readerRunnable, writerRunnable);
  }

  /**
   * Test whether a transaction can see changes from a merge that happens after the read (it should
   * not). transaction starts.
   *
   * @throws Throwable
   */
  @Test
  public void testRepeatableRead() throws Throwable {
    final Sequencer<ReaderRunnable.ReaderPhase> readerSequencer = new Sequencer<>(
        ReaderRunnable.ReaderPhase.class);
    final Sequencer<WriterRunnable.WriterPhase> writerSequencer = new Sequencer<>(
        WriterRunnable.WriterPhase.class);

    //Writer MERGE will wait reader get the snapshot
    writerSequencer.notify(WriterRunnable.WriterPhase.PRE_SNAPSHOT);
    writerSequencer.notify(WriterRunnable.WriterPhase.POST_SNAPSHOT);
    writerSequencer.notify(WriterRunnable.WriterPhase.POST_MERGE);

    //Reader will wait on an unorthodox way. It wont on its sequencer, but on the writer's one
    //and it will wait on the callback method
    readerSequencer.notify(ReaderRunnable.ReaderPhase.PRE_SNAPSHOT);
    readerSequencer.notify(ReaderRunnable.ReaderPhase.PRE_CALLBACK);

    WriterRunnable writerRunnable = new WriterRunnable(repository, writerSequencer) {

      @Override
      protected void postSnapshot(MutableMetaSnapshot snapshot) {
        snapshot.addMetaDatabase(dbName, dbId)
            .addMetaCollection(colName, colId);

        Assert.assertNotNull(snapshot.getMetaDatabaseByName(dbName));
        Assert.assertNotNull(snapshot.getMetaDatabaseByName(dbName).getMetaCollectionByIdentifier(
            colId));
      }

      @Override
      protected void postMerge(MutableMetaSnapshot snapshot) {
        //after merge, we should see the changes
        Assert.assertNotNull(snapshot.getMetaDatabaseByName(dbName));
        Assert.assertNotNull(snapshot.getMetaDatabaseByName(dbName).getMetaCollectionByIdentifier(
            colId));
      }
    };
    ReaderRunnable readerRunnable = new ReaderRunnable(repository, readerSequencer) {
      @Override
      protected void callback(ImmutableMetaSnapshot snapshot) {
        //first we check the initial state
        Assert.assertNull(snapshot.getMetaDatabaseByName(dbName));

        //after reader asserts, writer can merge
        writerSequencer.notify(WriterRunnable.WriterPhase.PRE_MERGE);
        //wait until writer merges
        writerSequencer.waitFor(WriterRunnable.WriterPhase.POST_MERGE);

        //after merge, we should not see the changes
        Assert.assertNull(snapshot.getMetaDatabaseByName(dbName));
      }
    };

    executeConcurrent(MILLIS_TO_WAIT, readerRunnable, writerRunnable);
  }

  /**
   * Tests whether two concurrent merges results on the union of changes.
   */
  @Test
  public void testTwoMerges() throws Throwable {
    final String colName2 = colName + "2";
    final String colId2 = colId + "2";

    final Sequencer<WriterRunnable.WriterPhase> writerSequencer1 = new Sequencer<>(
        WriterRunnable.WriterPhase.class);
    final Sequencer<WriterRunnable.WriterPhase> writerSequencer2 = new Sequencer<>(
        WriterRunnable.WriterPhase.class);
    final Sequencer<ReaderRunnable.ReaderPhase> readerSequencer = new Sequencer<>(
        ReaderRunnable.ReaderPhase.class);

    //Writer1 will wait for merge until writer2 is ready to merge. Then writer1 will merge and
    //after that, writer2 will merge.
    writerSequencer1.notify(WriterRunnable.WriterPhase.PRE_SNAPSHOT);
    writerSequencer1.notify(WriterRunnable.WriterPhase.POST_SNAPSHOT);
    writerSequencer1.notify(WriterRunnable.WriterPhase.POST_MERGE);

    writerSequencer2.notify(WriterRunnable.WriterPhase.PRE_SNAPSHOT);
    writerSequencer2.notify(WriterRunnable.WriterPhase.POST_SNAPSHOT);
    writerSequencer2.notify(WriterRunnable.WriterPhase.POST_MERGE);

    readerSequencer.notify(ReaderRunnable.ReaderPhase.PRE_CALLBACK);

    WriterRunnable writerRunnable1 = new WriterRunnable(repository, writerSequencer1) {
      @Override
      protected void postSnapshot(MutableMetaSnapshot snapshot) {
        snapshot.addMetaDatabase(dbName, dbId)
            .addMetaCollection(colName, colId);

        //it can see its changes
        Assert.assertNotNull(snapshot.getMetaDatabaseByName(dbName));
        Assert.assertNotNull(snapshot.getMetaDatabaseByName(dbName).getMetaCollectionByIdentifier(
            colId));

        //but not changes executed by the other thread
        Assert.assertNull(snapshot.getMetaDatabaseByName(dbName).getMetaCollectionByIdentifier(
            colId2));
      }

      @Override
      protected void postMerge(MutableMetaSnapshot snapshot) {
        //writer1 has finished its merge, so writer2 can start its own merge
        writerSequencer2.notify(WriterRunnable.WriterPhase.PRE_MERGE);
      }
    };

    WriterRunnable writerRunnable2 = new WriterRunnable(repository, writerSequencer2) {
      @Override
      protected void postSnapshot(MutableMetaSnapshot snapshot) {
        snapshot.addMetaDatabase(dbName, dbId)
            .addMetaCollection(colName2, colId2);

        //it can see its changes
        Assert.assertNotNull(snapshot.getMetaDatabaseByName(dbName));
        Assert.assertNotNull(snapshot.getMetaDatabaseByName(dbName).getMetaCollectionByIdentifier(
            colId2));

        //but not changes executed by the other thread
        Assert.assertNull(snapshot.getMetaDatabaseByName(dbName)
            .getMetaCollectionByIdentifier(colId));

        //writer2 has finished its modificiations, so writer1 can start its own merge
        writerSequencer1.notify(WriterRunnable.WriterPhase.PRE_MERGE);
      }

      @Override
      protected void postMerge(MutableMetaSnapshot snapshot) {
        //writer2 has finished his merge, so reader can start
        readerSequencer.notify(ReaderRunnable.ReaderPhase.PRE_SNAPSHOT);
      }

    };

    ReaderRunnable readerRunnable = new ReaderRunnable(repository, readerSequencer) {
      @Override
      protected void callback(ImmutableMetaSnapshot snapshot) {
        //lets check that both changes are seen after merge
        Assert.assertNotNull(snapshot.getMetaDatabaseByName(dbName));
        Assert.assertNotNull("Changes from writer1 are not seen",
            snapshot.getMetaDatabaseByName(dbName).getMetaCollectionByIdentifier(colId));

        Assert.assertNotNull("Changes from writer2 are not seen",
            snapshot.getMetaDatabaseByName(dbName).getMetaCollectionByIdentifier(colId2));
      }

    };

    executeConcurrent(MILLIS_TO_WAIT, writerRunnable1, writerRunnable2, readerRunnable);
  }

  private void executeConcurrent(long maxMillis, Runnable... runnables) throws TimeoutException,
      Throwable {
    assert runnables.length > 0 : "at least one runnable must be sent";

    ExecutorService es = Executors.newFixedThreadPool(runnables.length);

    List<Future<?>> futures = new ArrayList<>(runnables.length);
    for (Runnable runnable : runnables) {
      futures.add(es.submit(runnable));
    }
    try {
      for (Future<?> future : futures) {
        future.get(maxMillis, TimeUnit.MILLISECONDS);
      }
    } catch (ExecutionException ex) {
      throw ex.getCause();
    }
  }

  private static class ReaderRunnable implements Runnable {

    private final MvccMetainfoRepository repository;
    private final Sequencer<ReaderPhase> sequencer;

    public ReaderRunnable(MvccMetainfoRepository repository, Sequencer<ReaderPhase> sequencer) {
      this.repository = repository;
      this.sequencer = sequencer;
    }

    protected void preStart() {
    }

    protected void preSnapshot() {
    }

    protected void callback(ImmutableMetaSnapshot snapshot) {
    }

    @Override
    public void run() {
      ImmutableMetaSnapshot immutableSnapshot;

      preStart();

      sequencer.waitFor(ReaderPhase.PRE_SNAPSHOT);

      preSnapshot();

      try (SnapshotStage snapshotStage = repository.startSnapshotStage()) {
        immutableSnapshot = snapshotStage.createImmutableSnapshot();
      }

      sequencer.waitFor(ReaderPhase.PRE_CALLBACK);

      callback(immutableSnapshot);
    }

    private static enum ReaderPhase {
      PRE_SNAPSHOT,
      PRE_CALLBACK;
    }

  }

  private static class WriterRunnable implements Runnable {

    private final MvccMetainfoRepository repository;
    private final Sequencer<WriterPhase> sequencer;

    public WriterRunnable(MvccMetainfoRepository repository, Sequencer<WriterPhase> sequencer) {
      this.repository = repository;
      this.sequencer = sequencer;
    }

    protected void preStart() {
    }

    protected void preSnapshot() {
    }

    protected void postSnapshot(MutableMetaSnapshot snapshot) {
    }

    protected void preMerge() {
    }

    protected void postMerge(MutableMetaSnapshot snapshot) {
    }

    @Override
    public void run() {
      MutableMetaSnapshot mutableSnapshot;

      preStart();

      sequencer.waitFor(WriterPhase.PRE_SNAPSHOT);

      preSnapshot();

      try (SnapshotStage snapshotStage = repository.startSnapshotStage()) {
        mutableSnapshot = snapshotStage.createMutableSnapshot();
      }

      sequencer.waitFor(WriterPhase.POST_SNAPSHOT);

      postSnapshot(mutableSnapshot);

      sequencer.waitFor(WriterPhase.PRE_MERGE);

      preMerge();

      try (MergerStage mergeStage = repository.startMerge(mutableSnapshot)) {
        mergeStage.commit();
      } catch (UnmergeableException ex) {
        throw new AssertionError("Unmergeable changes", ex);
      }

      sequencer.waitFor(WriterPhase.POST_MERGE);

      postMerge(mutableSnapshot);
    }

    private static enum WriterPhase {
      PRE_SNAPSHOT,
      POST_SNAPSHOT,
      PRE_MERGE,
      POST_MERGE;
    }

  }
}
