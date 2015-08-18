
package com.torodb.torod.mongodb.repl;

import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.OplogOperationParser;
import com.eightkdata.mongowp.mongoserver.api.safe.oplog.OplogOperation;
import com.eightkdata.mongowp.mongoserver.pojos.OpTime;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.BadValueException;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.NoSuchKeyException;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.TypesMismatchException;
import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;
import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.torodb.torod.mongodb.repl.exceptions.EmptyOplogException;
import com.torodb.torod.mongodb.repl.exceptions.InvalidOplogOperation;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonInt32;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@NotThreadSafe
public class MongoOplogReader implements OplogReader {

    private static final Logger LOGGER
            = LoggerFactory.getLogger(MongoOplogReader.class);
    private static final String DATABASE = "local";
    private static final String COLLECTION = "oplog.rs";
    private final SyncSourceProvider ssp;
    private MongoClient client;
    private static final DocumentCodec CODEC = new DocumentCodec();

    @Inject
    public MongoOplogReader(SyncSourceProvider ssp) {
        this.ssp = ssp;
    }

    @Override
    public boolean connect(OpTime lastFetchedOpTime) {
        HostAndPort oldSyncSource = null;

        if (client != null) {
            String connectPoint = client.getConnectPoint();
            try {
                oldSyncSource = HostAndPort.fromString(connectPoint);
            } catch (IllegalArgumentException ex) {
                LOGGER.error(
                        "connection point from the old sync source is not "
                                + "parseable (value = {})",
                        connectPoint
                );
            }

            client.close();
        }
        HostAndPort hostAndPort = ssp.calculateSyncSource(oldSyncSource);

        client = new MongoClient(hostAndPort.getHostText(), hostAndPort.getPort());

        try {
            //test connection
            getLastOp();
        } catch (EmptyOplogException ex) {
            LOGGER.error("Error while connecting the sync source", ex);
            return false;
        } catch (InvalidOplogOperation ex) {
            LOGGER.error("Error while connecting the sync source", ex);
            return false;
        }

        return true;
    }

    @Override
    public OplogCursor queryGTE(OpTime lastFetchedOpTime) {
        Preconditions.checkState(client != null, "You have to connect this client before");

        BsonDocument query = new BsonDocument()
                .append("ts", new BsonDocument("$gte", lastFetchedOpTime.asBsonTimestamp())
                );

        MongoCollection<Document> collection = client
                .getDatabase(DATABASE)
                .getCollection(COLLECTION);

        MongoCursor<Document> iterator = collection
                .find(query)
                .cursorType(CursorType.TailableAwait)
                .iterator();

        return new MyOplogCursor(iterator);
    }

    @Override
    public OplogOperation getLastOp() throws EmptyOplogException, InvalidOplogOperation {
        Preconditions.checkState(client != null, "You have to connect this client before");

        MongoCollection<Document> collection = client
                .getDatabase(DATABASE)
                .getCollection(COLLECTION);

        Document lastDoc = collection.find()
                .sort(new BsonDocument().append("$natural", new BsonInt32(-1)))
                .first();

        if (lastDoc == null) {
            throw new EmptyOplogException();
        }

        BsonDocument document = new BsonDocumentWrapper(lastDoc, CODEC);

        try {
            return OplogOperationParser.fromBson(document);
        } catch (BadValueException ex) {
            throw new InvalidOplogOperation(document, ex);
        } catch (TypesMismatchException ex) {
            throw new InvalidOplogOperation(document, ex);
        } catch (NoSuchKeyException ex) {
            throw new InvalidOplogOperation(document, ex);
        }
    }

    @Override
    public OplogOperation getFirstOp() throws EmptyOplogException,
            InvalidOplogOperation {
        Preconditions.checkState(client != null, "You have to connect this client before");
        
        MongoCollection<Document> collection = client
                .getDatabase(DATABASE)
                .getCollection(COLLECTION);

        Document lastDoc = collection.find()
                .sort(new BsonDocument().append("$natural", new BsonInt32(1)))
                .first();

        if (lastDoc == null) {
            throw new EmptyOplogException();
        }

        BsonDocument document = new BsonDocumentWrapper(lastDoc, CODEC);

        try {
            return OplogOperationParser.fromBson(document);
        } catch (BadValueException ex) {
            throw new InvalidOplogOperation(document, ex);
        } catch (TypesMismatchException ex) {
            throw new InvalidOplogOperation(document, ex);
        } catch (NoSuchKeyException ex) {
            throw new InvalidOplogOperation(document, ex);
        }
    }

    @Override
    public boolean shouldChangeSyncSource() {
        return false;
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    private static class MyOplogCursor implements OplogCursor {
        private static final int MAX_IN_BATCH = 10;

        private final MongoCursor<Document> iterator;
        private OplogOperation nonConsumedNext;
        private int consumedInBatch = 0;
        private long batchTime;

        public MyOplogCursor(MongoCursor<Document> iterator) {
            this.iterator = iterator;
            this.consumedInBatch = 0;
        }

        @Override
        public boolean batchIsEmpty() {
            return consumedInBatch == MAX_IN_BATCH - 1;
        }

        @Override
        public OplogOperation nextInBatch() throws InvalidOplogOperation {
            if (batchIsEmpty()) {
                return null;
            }
            if (nonConsumedNext == null) {
                BsonDocumentWrapper doc = new BsonDocumentWrapper(iterator.next(), CODEC);
                try {
                    nonConsumedNext = OplogOperationParser.fromBson(doc);
                } catch (BadValueException ex) {
                    throw new InvalidOplogOperation(doc, ex);
                } catch (TypesMismatchException ex) {
                    throw new InvalidOplogOperation(doc, ex);
                } catch (NoSuchKeyException ex) {
                    throw new InvalidOplogOperation(doc, ex);
                }
            }
            assert nonConsumedNext != null;
            return nonConsumedNext;
        }

        @Override
        public OplogOperation consumeNextInBatch() throws InvalidOplogOperation {
            OplogOperation result = nextInBatch();
            if (result == null) {
                return null;
            }
            consumedInBatch = (consumedInBatch + 1 ) % MAX_IN_BATCH;
            nonConsumedNext = null;
            return result;
        }

        @Override
        public int getCurrentBatchedSize() {
            return MAX_IN_BATCH;
        }

        @Override
        public long getCurrentBatchTime() {
            return batchTime;
        }

        @Override
        public void newBatch(long maxWaitTime) {
            batchTime = System.currentTimeMillis();
            nonConsumedNext = null;
            consumedInBatch = 0;
        }

        @Override
        public boolean isDead() {
            return false;
        }

        @Override
        public void close() {
            iterator.close();
        }
    }



}
