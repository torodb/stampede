
package com.torodb.torod.mongodb.repl;

import com.eightkdata.mongowp.mongoserver.api.safe.oplog.OplogOperation;
import com.eightkdata.mongowp.mongoserver.pojos.OpTime;
import java.io.Closeable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

/**
 *
 */
class OplogManager {

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private long lastAppliedHash;
    private OpTime lastAppliedOpTime;

    ReadTransaction createReadTransaction() {
        return new ReadTransaction(lock.readLock());
    }

    WriteTransaction createWriteTransaction() {
        return new WriteTransaction(lock.writeLock());
    }

    @NotThreadSafe
    public class ReadTransaction implements Closeable {
        private final Lock readLock;
        private boolean closed;

        private ReadTransaction(Lock readLock) {
            this.readLock = readLock;
            readLock.lock();
            closed = false;
        }

        public long getLastAppliedHash() {
            if (closed) {
                throw new IllegalStateException("Transaction closed");
            }
            return lastAppliedHash;
        }

        public OpTime getLastAppliedOptime() {
            if (closed) {
                throw new IllegalStateException("Transaction closed");
            }
            return lastAppliedOpTime;
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                readLock.unlock();
            }
        }
    }

    @NotThreadSafe
    class WriteTransaction implements Closeable {
        private final Lock writeLock;
        private boolean closed = false;

        public WriteTransaction(Lock writeLock) {
            this.writeLock = writeLock;
            writeLock.lock();
            closed = false;
        }

        public long getLastAppliedHash() {
            if (closed) {
                throw new IllegalStateException("Transaction closed");
            }
            return lastAppliedHash;
        }

        public OpTime getLastAppliedOptime() {
            if (closed) {
                throw new IllegalStateException("Transaction closed");
            }
            return lastAppliedOpTime;
        }

        public void addOperation(@Nonnull OplogOperation op) {
            if (closed) {
                throw new IllegalStateException("Transaction closed");
            }
            lastAppliedHash = op.getHash();
            lastAppliedOpTime = op.getOptime();
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                writeLock.unlock();
            }
        }


    }
}
