
package com.torodb.torod.mongodb.repl;

import com.eightkdata.mongowp.OpTime;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.messages.request.QueryMessage.QueryOption;
import com.eightkdata.mongowp.messages.request.QueryMessage.QueryOptions;
import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.DeleteCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.DeleteCommand.DeleteArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.DeleteCommand.DeleteStatement;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.InsertCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.InsertCommand.InsertArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.InsertCommand.InsertResult;
import com.eightkdata.mongowp.utils.BsonDocumentBuilder;
import com.eightkdata.mongowp.utils.BsonReaderTool;
import com.google.common.base.Preconditions;
import com.google.common.primitives.UnsignedInteger;
import com.google.common.util.concurrent.AbstractIdleService;
import com.torodb.torod.core.annotations.DatabaseName;
import com.torodb.torod.mongodb.annotations.Locked;
import com.torodb.torod.mongodb.annotations.MongoDBLayer;
import com.torodb.torod.mongodb.impl.LocalMongoClient;
import com.torodb.torod.mongodb.impl.LocalMongoConnection;
import java.io.Closeable;
import java.util.EnumSet;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.eightkdata.mongowp.bson.utils.DefaultBsonValues.*;

/**
 *
 */
@Singleton
public class OplogManager extends AbstractIdleService {

    private static final Logger LOGGER
            = LoggerFactory.getLogger(OplogManager.class);
    private static final String KEY = "lastAppliedOplogEntry";
    private static final BsonDocument DOC_QUERY = newDocument(KEY, newDocument("$exists", TRUE));
    
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private long lastAppliedHash;
    private OpTime lastAppliedOpTime;
    private final Executor executor;
    private final LocalMongoClient localClient;
    private final String supportedDatabase;

    @Inject
    public OplogManager(@MongoDBLayer Executor executor, LocalMongoClient localClient, @DatabaseName String supportedDatabase) {
        this.executor = executor;
        this.localClient = localClient;
        this.supportedDatabase = supportedDatabase;
    }

    ReadTransaction createReadTransaction() {
        Preconditions.checkState(isRunning(), "The service is not running");
        return new ReadTransaction(lock.readLock());
    }

    WriteTransaction createWriteTransaction() {
        Preconditions.checkState(isRunning(), "The service is not running");
        return new WriteTransaction(lock.writeLock());
    }

    @Override
    protected Executor executor() {
        return executor;
    }

    @Override
    protected void startUp() throws Exception {
        LOGGER.debug("Starting OplogManager");
        Lock mutex = lock.writeLock();
        mutex.lock();
        try {
            loadState();
        } finally {
            mutex.unlock();
        }
        LOGGER.debug("Started OplogManager");
    }

    @Override
    protected void shutDown() throws Exception {
        LOGGER.debug("Stopping OplogManager");
    }

    @Locked(exclusive = false)
    private void storeState(long hash, OpTime opTime) throws OplogManagerPersistException {
        Preconditions.checkState(isRunning(), "The service is not running");

        try (LocalMongoConnection connection = localClient.openConnection()) {
            connection.execute(
                    DeleteCommand.INSTANCE,
                    supportedDatabase,
                    true,
                    new DeleteArgument.Builder("torodb")
                            .addStatement(new DeleteStatement(DOC_QUERY, false))
                            .build()
            );


            InsertResult insertResult = connection.execute(
                    InsertCommand.INSTANCE,
                    supportedDatabase,
                    true,
                    new InsertArgument.Builder("torodb")
                        .addDocument(
                                new BsonDocumentBuilder()
                                        .appendUnsafe(KEY, new BsonDocumentBuilder()
                                                .appendUnsafe("hash", newLong(hash))
                                                .appendUnsafe("opTime", new BsonDocumentBuilder()
                                                        .appendUnsafe("t", newLong(opTime.getSecs().longValue()))
                                                        .appendUnsafe("i", newLong(opTime.getTerm().longValue()))
                                                        .build()
                                                ).build()
                                        ).build()
                                    ).build()
            );
            if (insertResult.getN() != 1) {
                throw new OplogManagerPersistException();
            }
        } catch (MongoException ex) {
            throw new OplogManagerPersistException(ex);
        }
    }

    @Locked(exclusive = true)
    private void loadState() throws OplogManagerPersistException {
        try (LocalMongoConnection connection = localClient.openConnection()) {
            EnumSet<QueryOption> flags = EnumSet.of(QueryOption.SLAVE_OK);
            BsonDocument doc = connection.query(
                    supportedDatabase,
                    "torodb",
                    DOC_QUERY,
                    0,
                    0,
                    new QueryOptions(flags),
                    null
            )
                    .getOne();
            if (doc == null) {
                lastAppliedHash = 0;
                lastAppliedOpTime = OpTime.EPOCH;
            }
            else {
                BsonDocument subDoc = BsonReaderTool.getDocument(doc, KEY);
                lastAppliedHash = BsonReaderTool.getLong(subDoc, "hash");

                BsonDocument opTimeDoc = BsonReaderTool.getDocument(subDoc, "opTime");
                lastAppliedOpTime = new OpTime(
                        UnsignedInteger.valueOf(BsonReaderTool.getLong(opTimeDoc, "t")),
                        UnsignedInteger.valueOf(BsonReaderTool.getLong(opTimeDoc, "i"))
                );
            }
        } catch (MongoException ex) {
            throw new OplogManagerPersistException(ex);
        }
    }

    public static class OplogManagerPersistException extends Exception {
        private static final long serialVersionUID = 1L;

        public OplogManagerPersistException() {
        }

        public OplogManagerPersistException(Throwable cause) {
            super(cause);
        }

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

        @Nonnull
        public OpTime getLastAppliedOptime() {
            if (closed) {
                throw new IllegalStateException("Transaction closed");
            }
            if (lastAppliedOpTime == null) {
                throw new AssertionError("lastAppliedOpTime should not be null");
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
    public class WriteTransaction implements Closeable {
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

        public void addOperation(@Nonnull OplogOperation op) throws OplogManagerPersistException {
            if (closed) {
                throw new IllegalStateException("Transaction closed");
            }

            storeState(op.getHash(), op.getOpTime());

            lastAppliedHash = op.getHash();
            lastAppliedOpTime = op.getOpTime();
        }

        public void forceNewValue(long newHash, OpTime newOptime) throws OplogManagerPersistException {
            if (closed) {
                throw new IllegalStateException("Transaction closed");
            }
            storeState(newHash, newOptime);

            OplogManager.this.lastAppliedHash = newHash;
            OplogManager.this.lastAppliedOpTime = newOptime;
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                writeLock.unlock();
            }
        }

        /**
         * Deletes all information on the current oplog and reset all its
         * variables (like lastAppliedHash or lastAppliedOptime).
         */
        void truncate() throws OplogManagerPersistException {
            if (closed) {
                throw new IllegalStateException("Transaction closed");
            }
            storeState(0, OpTime.EPOCH);

            lastAppliedHash = 0;
            lastAppliedOpTime = OpTime.EPOCH;
        }


    }
}
