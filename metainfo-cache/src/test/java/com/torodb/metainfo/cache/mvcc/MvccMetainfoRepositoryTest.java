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
 * along with metainfo-cache. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 *
 */
package com.torodb.metainfo.cache.mvcc;

import com.torodb.common.util.Sequencer;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MetainfoRepository.MergerStage;
import com.torodb.core.transaction.metainf.MetainfoRepository.SnapshotStage;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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

    private static final long MILLIS_TO_WAIT = 10_000;

    public MvccMetainfoRepositoryTest() {
    }

    @Before
    public void setUp() throws Exception {
        repository = new MvccMetainfoRepository();
    }

    @Test
    public void testSingleThread() {
        MutableMetaSnapshot mutableSnapshot;

        try (SnapshotStage snapshotStage = repository.startSnapshotStage()) {
            mutableSnapshot = snapshotStage.createMutableSnapshot();
        }

        mutableSnapshot.addMetaDatabase(dbName, dbId)
                .addMetaCollection(colName, colId);

        Assert.assertNotNull(mutableSnapshot.getMetaDatabaseByName(dbName));
        Assert.assertNotNull(mutableSnapshot.getMetaDatabaseByName(dbName).getMetaCollectionByIdentifier(colId));

        try (MergerStage mergeStage = repository.startMerge(mutableSnapshot)) {
        }

        ImmutableMetaSnapshot immutableSnapshot;
        try (SnapshotStage snapshotStage = repository.startSnapshotStage()) {
            immutableSnapshot = snapshotStage.createImmutableSnapshot();
        }

        Assert.assertNotNull(immutableSnapshot.getMetaDatabaseByName(dbName));
        Assert.assertNotNull(immutableSnapshot.getMetaDatabaseByName(dbName).getMetaCollectionByIdentifier(colId));
    }

    /**
     * Tests if changes on a thread are seen by another thread after a merge phase.
     * 
     * @throws Throwable
     */
    @Test
    public void testSimpleReadWriter() throws Throwable {
        final Sequencer<ReaderRunnable.ReaderPhase> readerSequencer = new Sequencer<>(ReaderRunnable.ReaderPhase.class);
        final Sequencer<WriterRunnable.WriterPhase> writerSequencer = new Sequencer<>(WriterRunnable.WriterPhase.class);

        writerSequencer.notify(WriterRunnable.WriterPhase.values());

        WriterRunnable writerRunnable = new WriterRunnable(repository, writerSequencer) {
            @Override
            protected void postSnapshot(MutableMetaSnapshot snapshot) {
                snapshot.addMetaDatabase(dbName, dbId)
                        .addMetaCollection(colName, colId);
            }

            @Override
            protected void postMerge() {
                readerSequencer.notify(ReaderRunnable.ReaderPhase.values());
            }
            
        };
        ReaderRunnable readerRunnable = new ReaderRunnable(repository, readerSequencer) {
            @Override
            protected void callback(ImmutableMetaSnapshot snapshot) {
                Assert.assertNotNull(snapshot.getMetaDatabaseByName(dbName));
                Assert.assertNotNull(snapshot.getMetaDatabaseByName(dbName).getMetaCollectionByIdentifier(colId));
            }
        };

        executeConcurrent(MILLIS_TO_WAIT, readerRunnable, writerRunnable);
    }

    /**
     * Test whether modifications are seen before merge finishes.
     * @throws Throwable
     */
    @Test
    public void testReadCommited() throws Throwable {
        final Sequencer<ReaderRunnable.ReaderPhase> readerSequencer = new Sequencer<>(ReaderRunnable.ReaderPhase.class);
        final Sequencer<WriterRunnable.WriterPhase> writerSequencer = new Sequencer<>(WriterRunnable.WriterPhase.class);

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

    private void executeConcurrent(long maxMillis, Runnable... runnables) throws TimeoutException, Throwable {
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

        protected void preStart() {}
        protected void preSnapshot() {}
        protected void callback(ImmutableMetaSnapshot snapshot) {}

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

        protected void preStart() {}
        protected void preSnapshot() {}
        protected void postSnapshot(MutableMetaSnapshot snapshot) {}
        protected void preMerge() {}
        protected void postMerge() {}


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
            }

            sequencer.waitFor(WriterPhase.POST_MERGE);
            
            postMerge();
        }

        private static enum WriterPhase {
            PRE_SNAPSHOT,
            POST_SNAPSHOT,
            PRE_MERGE,
            POST_MERGE;
        }

    }
}
