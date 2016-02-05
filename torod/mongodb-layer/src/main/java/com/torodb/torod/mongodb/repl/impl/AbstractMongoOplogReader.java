
package com.torodb.torod.mongodb.repl.impl;

import com.eightkdata.mongowp.OpTime;
import com.eightkdata.mongowp.bson.BsonArray;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonInt32;
import com.eightkdata.mongowp.client.core.MongoConnection;
import com.eightkdata.mongowp.exceptions.*;
import com.eightkdata.mongowp.messages.request.QueryMessage.QueryOption;
import com.eightkdata.mongowp.messages.request.QueryMessage.QueryOptions;
import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.eightkdata.mongowp.server.api.pojos.MongoCursor;
import com.eightkdata.mongowp.server.api.pojos.MongoCursor.Batch;
import com.eightkdata.mongowp.server.api.pojos.MongoCursor.DeadCursorException;
import com.eightkdata.mongowp.server.api.pojos.TransformationMongoCursor;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.OplogOperationParser;
import com.eightkdata.mongowp.utils.BsonArrayBuilder;
import com.eightkdata.mongowp.utils.BsonDocumentBuilder;
import com.google.common.base.Preconditions;
import com.torodb.torod.mongodb.repl.OplogReader;
import java.util.EnumSet;
import java.util.Iterator;

import static com.eightkdata.mongowp.bson.utils.DefaultBsonValues.*;

/**
 *
 */
public abstract class AbstractMongoOplogReader implements OplogReader {
    private static final String DATABASE = "local";
    private static final String COLLECTION = "oplog.rs";

    private static final BsonDocument NATURAL_ORDER_SORT = newDocument("$natural", newInt(1));
    private static final BsonDocument INVERSE_ORDER_SORT = newDocument("$natural", newInt(-1));

    protected abstract MongoConnection consumeConnection();
    protected abstract void releaseConnection(MongoConnection connection);
    
    @Override
    public MongoCursor<OplogOperation> queryGTE(OpTime lastFetchedOpTime) throws MongoException {
        BsonDocument query = newDocument(
                "ts",
                newDocument("$gte", lastFetchedOpTime.asBsonTimestamp())
        );
        query = new BsonDocumentBuilder()
                .appendUnsafe("$query", query)
                .appendUnsafe("$orderby", NATURAL_ORDER_SORT)
                .build();

        EnumSet<QueryOption> flags = EnumSet.of(
                QueryOption.AWAIT_DATA,
                QueryOption.TAILABLE_CURSOR
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
        BsonArrayBuilder conditions = new BsonArrayBuilder();
        conditions.add(
                newDocument(
                        "ts",
                        newDocument(includeFrom ? "$gte" : "$gt", from.asBsonTimestamp())
                )
        );
        conditions.add(
                newDocument(
                        "ts",
                        newDocument(includeTo ? "$lte" : "$lt", to.asBsonTimestamp())
                )
        );
        BsonDocument query = new BsonDocumentBuilder()
                .appendUnsafe("$query", newDocument("$and", conditions.build()))
                .appendUnsafe("$orderby", NATURAL_ORDER_SORT)
                .build();

        EnumSet<QueryOption> flags = EnumSet.noneOf(QueryOption.class);

        return query(query, flags);
    }

    @Override
    public boolean shouldChangeSyncSource() {
        return false;
    }

    public MongoCursor<OplogOperation> query(BsonDocument query, EnumSet<QueryOption> flags) throws MongoException {
        Preconditions.checkState(!isClosed(), "You have to connect this client before");

        MongoConnection connection = consumeConnection();
        MongoCursor<BsonDocument> cursor = connection.query(
                DATABASE,
                COLLECTION,
                query,
                0,
                0,
                new QueryOptions(flags),
                null
        );

        return new MyCursor<>(
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

        BsonDocument query = new BsonDocumentBuilder()
                .appendUnsafe("$query", EMPTY_DOC)
                .appendUnsafe("$orderby", first ? NATURAL_ORDER_SORT : INVERSE_ORDER_SORT)
                .build();

        EnumSet<QueryOption> flags = EnumSet.of(QueryOption.SLAVE_OK);

        BsonDocument doc;
        MongoConnection connection = consumeConnection();
        try {
            MongoCursor<BsonDocument> cursor = connection.query(
                    DATABASE,
                    COLLECTION,
                    query,
                    0,
                    1,
                    new QueryOptions(flags),
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
            } catch (BadValueException |
                    TypesMismatchException |
                    NoSuchKeyException ex) {
                throw new OplogOperationUnsupported(doc, ex);
            }
        } finally {
            releaseConnection(connection);
        }
    }

    private class MyCursor<T> implements MongoCursor<T> {
        private final MongoConnection connection;
        private final MongoCursor<T> delegate;

        private MyCursor(MongoConnection connection, MongoCursor<T> delegate) {
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
