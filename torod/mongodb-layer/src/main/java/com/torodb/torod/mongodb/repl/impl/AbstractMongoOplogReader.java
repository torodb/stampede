
package com.torodb.torod.mongodb.repl.impl;

import com.eightkdata.mongowp.client.core.MongoConnection;
import com.eightkdata.mongowp.messages.request.QueryMessage;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.OplogOperationParser;
import com.eightkdata.mongowp.mongoserver.api.safe.oplog.OplogOperation;
import com.eightkdata.mongowp.mongoserver.api.safe.pojos.MongoCursor;
import com.eightkdata.mongowp.mongoserver.api.safe.pojos.MongoCursor.Batch;
import com.eightkdata.mongowp.mongoserver.api.safe.pojos.MongoCursor.DeadCursorException;
import com.eightkdata.mongowp.mongoserver.api.safe.pojos.TransformationMongoCursor;
import com.eightkdata.mongowp.mongoserver.pojos.OpTime;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.*;
import com.google.common.base.Preconditions;
import com.torodb.torod.mongodb.repl.OplogReader;
import java.util.EnumSet;
import java.util.Iterator;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;

/**
 *
 */
public abstract class AbstractMongoOplogReader implements OplogReader {
    private static final String DATABASE = "local";
    private static final String COLLECTION = "oplog.rs";

    private static final BsonDocument NATURAL_ORDER_SORT = new BsonDocument()
            .append("$natural", new BsonInt32(1));
    private static final BsonDocument INVERSE_ORDER_SORT = new BsonDocument()
            .append("$natural", new BsonInt32(-1));

    protected abstract MongoConnection consumeConnection();
    protected abstract void releaseConnection(MongoConnection connection);
    
    @Override
    public MongoCursor<OplogOperation> queryGTE(OpTime lastFetchedOpTime) throws MongoException {
        BsonDocument query = new BsonDocument(
                "ts",
                new BsonDocument("$gte", lastFetchedOpTime.asBsonTimestamp())
        );
        query = new BsonDocument()
                .append("$query", query)
                .append("$orderby", NATURAL_ORDER_SORT);

        EnumSet<QueryMessage.Flag> flags = EnumSet.of(
                QueryMessage.Flag.AWAIT_DATA, 
                QueryMessage.Flag.TAILABLE_CURSOR
        );

        return query(query, flags);
    }

    @Override
    public OplogOperation getLastOp() throws
            OplogStartMissingException,
            OplogOperationUnsupported,
            MongoException {
        return getFirstOrLastOp(false);
    }

    @Override
    public OplogOperation getFirstOp() throws
            OplogStartMissingException,
            OplogOperationUnsupported,
            MongoException {
        return getFirstOrLastOp(true);
    }
    @Override
    public MongoCursor<OplogOperation> between(
            OpTime from, boolean includeFrom,
            OpTime to, boolean includeTo) throws MongoException {
        BsonArray conditions = new BsonArray();
        conditions.add(new BsonDocument()
                .append("ts", 
                        new BsonDocument(includeFrom ? "$gte" : "$gt", from.asBsonTimestamp())
                )
        );
        conditions.add(new BsonDocument()
                .append("ts", 
                        new BsonDocument(includeTo ? "$lte" : "$lt", to.asBsonTimestamp())
                )
        );
        BsonDocument query = new BsonDocument()
                .append("$query", new BsonDocument("$and", conditions))
                .append("$orderby", NATURAL_ORDER_SORT);

        EnumSet<QueryMessage.Flag> flags = EnumSet.noneOf(QueryMessage.Flag.class);

        return query(query, flags);
    }

    @Override
    public boolean shouldChangeSyncSource() {
        return false;
    }

    public MongoCursor<OplogOperation> query(BsonDocument query, EnumSet<QueryMessage.Flag> flags) throws MongoException {
        Preconditions.checkState(!isClosed(), "You have to connect this client before");

        MongoConnection connection = consumeConnection();
        MongoCursor<BsonDocument> cursor = connection.query(
                DATABASE,
                COLLECTION,
                flags,
                query,
                0,
                0,
                null
        );

        return new MyCursor<OplogOperation>(
                connection,
                TransformationMongoCursor.create(
                        cursor,
                        OplogOperationParser.asFunction()
                )
        );
    }

    private OplogOperation getFirstOrLastOp(boolean first) throws
            OplogStartMissingException,
            OplogOperationUnsupported,
            MongoException {
        Preconditions.checkState(!isClosed(), "You have to connect this client before");

        BsonDocument query = new BsonDocument()
                .append("$query", new BsonDocument())
                .append("$orderby", first ? NATURAL_ORDER_SORT : INVERSE_ORDER_SORT);

        EnumSet<QueryMessage.Flag> flags = EnumSet.of(QueryMessage.Flag.SLAVE_OK);

        BsonDocument doc;
        MongoConnection connection = consumeConnection();
        try {
            MongoCursor<BsonDocument> cursor = connection.query(
                    DATABASE,
                    COLLECTION,
                    flags,
                    query,
                    0,
                    1,
                    null
            );
            try {
                Batch<BsonDocument> batch = cursor.fetchBatch();
                try {
                    if (!batch.hasNext()) {
                        throw new OplogStartMissingException(getSyncSource());
                    }
                    doc = batch.next();
                } finally {
                    batch.close();
                }
            } finally {
                cursor.close();
            }

            try {
                return OplogOperationParser.fromBson(doc);
            } catch (BadValueException ex) {
                throw new OplogOperationUnsupported(doc, ex);
            } catch (TypesMismatchException ex) {
                throw new OplogOperationUnsupported(doc, ex);
            } catch (NoSuchKeyException ex) {
                throw new OplogOperationUnsupported(doc, ex);
            }
        } finally {
            releaseConnection(connection);
        }
    }

    private class MyCursor<T> implements MongoCursor<T> {
        private final MongoConnection connection;
        private final MongoCursor<T> delegate;

        public MyCursor(MongoConnection connection, MongoCursor<T> delegate) {
            this.connection = connection;
            this.delegate = delegate;
        }

        @Override
        public String getDatabase() {
            return delegate.getDatabase();
        }

        @Override
        public String getCollection() {
            return delegate.getCollection();
        }

        @Override
        public long getId() {
            return delegate.getId();
        }

        @Override
        public void setMaxBatchSize(int newBatchSize) {
            delegate.setMaxBatchSize(newBatchSize);
        }

        @Override
        public int getMaxBatchSize() {
            return delegate.getMaxBatchSize();
        }

        @Override
        public boolean isTailable() {
            return delegate.isTailable();
        }

        @Override
        public boolean isDead() {
            return delegate.isDead();
        }

        @Override
        public Batch<T> fetchBatch() throws MongoException,
                DeadCursorException {
            return delegate.fetchBatch();
        }

        @Override
        public T getOne() throws MongoException, DeadCursorException {
            T one = delegate.getOne();
            releaseConnection(connection);
            return one;
        }

        @Override
        public void close() {
            delegate.close();
            releaseConnection(connection);
        }

        @Override
        public Iterator<T> iterator() {
            return delegate.iterator();
        }
    }
}
