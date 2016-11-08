/*
 * MongoWP - ToroDB-poc: Torod Layer
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.torod.impl.memory;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.torodb.core.document.ToroDocument;
import com.torodb.core.exceptions.user.UniqueIndexViolationException;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.torod.IndexInfo;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 *
 */
@ThreadSafe
public class MemoryData {

    private Table<String, String, Map<Integer, KVDocument>> data = HashBasedTable.create();
    private Table<String, String, Map<String, IndexInfo>> indexes = HashBasedTable.create();
    private final AtomicInteger idGenerator = new AtomicInteger();
    private ReadWriteLock lock = new ReentrantReadWriteLock(true);

    public MDReadTransaction openReadTransaction() {
        lock.readLock().lock();
        try {
            return new MDReadTransaction(data, indexes);
        } finally {
            lock.readLock().unlock();
        }
    }

    @SuppressFBWarnings(value = {"UL_UNRELEASED_LOCK"})
    public MDWriteTransaction openWriteTransaction() {
        Lock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            return new MDWriteTransaction(data, indexes, () -> idGenerator.incrementAndGet(), this::onCommit, writeLock);
        } catch (Throwable ex) {
            writeLock.unlock();
            throw ex;
        }
    }

    private void onCommit(MDTransaction trans) {
        this.data = trans.getData();
        this.indexes = trans.getIndexes();
    }

    public static class MDTransaction implements AutoCloseable {

        private boolean closed = false;
        Table<String, String, Map<Integer, KVDocument>> data;
        Table<String, String, Map<String, IndexInfo>> indexes;

        public MDTransaction(Table<String, String, Map<Integer, KVDocument>> data,
                Table<String, String, Map<String, IndexInfo>> indexes) {
            this.data = data;
            this.indexes = indexes;
        }

        public Table<String, String, Map<Integer, KVDocument>> getData() {
            return data;
        }

        public Table<String, String, Map<String, IndexInfo>> getIndexes() {
            return indexes;
        }
        
        public boolean isClosed() {
            return closed;
        }

        public boolean existsDatabase(String db) {
            return data.containsRow(db);
        }

        public boolean existsCollection(String db, String col) {
            return data.contains(db, col);
        }

        public Stream<ToroDocument> streamCollection(String db, String col) {
            Map<Integer, KVDocument> map = data.get(db, col);
            if (map == null) {
                return Stream.empty();
            }
            return map.entrySet().stream()
                    .map(this::entryToDocument);
        }

        public Stream<String> streamDbs() {
            return data.rowKeySet().stream();
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
            }
        }

        public Stream<ToroDocument> streamAllDocs() {
            return data.values().stream().flatMap(map -> map.entrySet().stream()).map(this::entryToDocument);
        }

        private ToroDocument entryToDocument(Map.Entry<Integer, KVDocument> entry) {
            return new ToroDocument(entry.getKey(), entry.getValue());
        }

    }

    @NotThreadSafe
    public static class MDReadTransaction extends MDTransaction{

        public MDReadTransaction(Table<String, String, Map<Integer, KVDocument>> data,
                Table<String, String, Map<String, IndexInfo>> indexes) {
            super(data, indexes);
        }

    }

    @NotThreadSafe
    public static class MDWriteTransaction extends MDTransaction {

        final Table<String, String, Map<Integer, KVDocument>> initialData;
        private final IntSupplier idGenerator;
        private final Consumer<MDTransaction> commitConsumer;
        private final Lock lock;

        public MDWriteTransaction(
                Table<String, String, Map<Integer, KVDocument>> data,
                Table<String, String, Map<String, IndexInfo>> indexes,
                IntSupplier idGenerator,
                Consumer<MDTransaction> commitConsumer,
                Lock lock) {
            super(HashBasedTable.create(data), HashBasedTable.create(indexes));
            this.idGenerator = idGenerator;
            this.commitConsumer = commitConsumer;
            this.lock = lock;
            this.initialData = data;
        }

        public void clear() {
            Preconditions.checkState(!isClosed(), "This transaction is closed");
            data.clear();
        }

        Map<Integer, KVDocument> getMap(String db, String col) {
            Map<Integer, KVDocument> map = data.get(db, col);
            if (map == null) {
                map = new HashMap<>();
                data.put(db, col, map);
            }
            return map;
        }

        void insert(String db, String col, Stream<KVDocument> docs) throws UniqueIndexViolationException {
            Map<Integer, KVDocument> map = getMap(db, col);
            List<KVDocument> docList = docs.collect(Collectors.toList());

            Optional<KVDocument> repeatedDoc = docList.stream().filter(doc -> {
                KVValue<?> mongoId = doc.get("_id");
                if (mongoId != null) {
                    Optional<KVDocument> withSameId = map.values().stream()
                            .filter(otherDoc -> mongoId.equals(otherDoc.get("_id")))
                            .findAny();
                    return withSameId.isPresent();
                }
                return false;
            }).findAny();
            if (repeatedDoc.isPresent()) {
                throw new UniqueIndexViolationException("_id", repeatedDoc.get());
            }

            docList.forEach(doc -> {
                int id = idGenerator.getAsInt();
                map.put(id, doc);
            });
        }

        long delete(String dbName, String colName, Stream<Integer> dids) {
            Map<Integer, KVDocument> map = getMap(dbName, colName);
            return dids.map(did -> {
                return map.remove(did) != null;
            })
                    .filter(b -> b)
                    .count();
        }

        long deleteAll(String dbName, String colName) {
            Map<Integer, KVDocument> map = data.get(dbName, colName);
            if (map == null) {
                return 0;
            }
            int count = map.size();
            map.clear();
            return count;
        }

        void dropCollection(String dbName, String colName) {
            data.remove(dbName, colName);
        }

        void renameCollection(String fromDb, String fromCollection, String toDb, String toCollection) {
            Map<Integer, KVDocument> col = data.remove(fromDb, fromCollection);
            if (col != null) {
                data.put(toDb, toCollection, col);
            }
        }

        void createCollection(String dbName, String colName) {
            getMap(dbName, colName);
        }

        void dropDatabase(String dbName) {
            HashSet<String> columns = new HashSet<>(data.row(dbName).keySet());

            for (String colName : columns) {
                data.remove(dbName, colName);
            }
        }

        void rollback() {
            this.data = initialData;
        }

        void commit() {
            commitConsumer.accept(this);
        }

        @Override
        public void close() {
            if (!isClosed()) {
                lock.unlock();
            }
            super.close();
        }

    }

}
