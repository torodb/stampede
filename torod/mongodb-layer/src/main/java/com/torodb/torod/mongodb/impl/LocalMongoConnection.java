
package com.torodb.torod.mongodb.impl;

import com.eightkdata.mongowp.client.core.MongoConnection;
import com.eightkdata.mongowp.messages.request.InsertMessage.Flag;
import com.eightkdata.mongowp.messages.request.*;
import com.eightkdata.mongowp.messages.response.ReplyMessage;
import com.eightkdata.mongowp.mongoserver.api.safe.*;
import com.eightkdata.mongowp.mongoserver.api.safe.pojos.MongoCursor;
import com.eightkdata.mongowp.mongoserver.api.safe.pojos.MongoCursor.Batch;
import com.eightkdata.mongowp.mongoserver.api.safe.pojos.MongoCursor.MongoCursorIterator;
import com.eightkdata.mongowp.mongoserver.api.safe.pojos.QueryRequest;
import com.eightkdata.mongowp.mongoserver.api.safe.pojos.SimpleBatch;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.CursorNotFoundException;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.MongoException;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.UnknownErrorException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.torodb.torod.mongodb.annotations.Local;
import io.netty.util.DefaultAttributeMap;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import javax.inject.Inject;
import org.bson.BsonDocument;
import org.slf4j.LoggerFactory;

/**
 * A low level local {@link MongoConnection}.
 * <p>
 * Low level means it is just a wrapper to ToroDB related objects, so no
 * replication or ;ongoDB related concurrency checks are done, the oplog is not
 * modified, etc. It is designed to be used as a utility by replication logic
 * classes.
 */
public class LocalMongoConnection implements MongoConnection  {
    private static final org.slf4j.Logger LOGGER
            = LoggerFactory.getLogger(LocalMongoConnection.class);

    private final LocalMongoClient owner;
    private final Connection connection;
    private final SafeRequestProcessor processor;
    private final CommandsExecutor commandsExecutor;
    private int requestCounter;
    private boolean closed = false;

    @Inject
    public LocalMongoConnection(
            LocalMongoClient owner,
            @Local SafeRequestProcessor processor,
            CommandsExecutor commandsExecutor) {
        this.owner = owner;
        this.processor = processor;
        requestCounter = 0;
        this.commandsExecutor = commandsExecutor;

        this.connection = new Connection(requestCounter, new DefaultAttributeMap());
        processor.onConnectionActive(connection);
    }

    @Override
    public LocalMongoClient getClientOwner() {
        return owner;
    }

    private Request newRequest(String database) {
        return new Request(
                connection,
                requestCounter++,
                database,
                null,
                0
        );
    }

    private RequestBaseMessage newBaseMessage(Request request) {
        return new RequestBaseMessage(null, 0, request.getRequestId());
    }

    @Override
    public MongoCursor<BsonDocument> query(
            String database,
            String collection,
            EnumSet<QueryMessage.Flag> flags,
            BsonDocument query,
            int numberToSkip,
            int numberToReturn,
            BsonDocument projection) throws MongoException {
        Preconditions.checkState(!closed, "This client is closed");

        Request request = newRequest(database);
        QueryRequest queryRequest = new QueryRequest.Builder(database, collection)
                .setAutoclose(!flags.contains(QueryMessage.Flag.TAILABLE_CURSOR))
                .setAwaitData(flags.contains(QueryMessage.Flag.AWAIT_DATA))
                .setExhaust(flags.contains(QueryMessage.Flag.EXHAUST))
                .setLimit(numberToReturn)
                .setNoCursorTimeout(flags.contains(QueryMessage.Flag.NO_CURSOR_TIMEOUT))
                .setNumberToSkip(numberToSkip)
                .setOplogReplay(flags.contains(QueryMessage.Flag.OPLOG_REPLAY))
                .setPartial(flags.contains(QueryMessage.Flag.PARTIAL))
                .setProjection(projection)
                .setQuery(query)
                .setSlaveOk(flags.contains(QueryMessage.Flag.SLAVE_OK))
                .setTailable(flags.contains(QueryMessage.Flag.TAILABLE_CURSOR))
                .build();


        ReplyMessage reply = processor.query(request, queryRequest);
        return new MyCursor(
                reply.getCursorId(),
                database,
                collection,
                queryRequest.isTailable(),
                reply.getDocuments()
        );
    }

    @Override
    public void asyncKillCursors(Iterable<Long> cursors) throws
            MongoException {

        int lenght = Iterables.size(cursors);
        long[] cursorsArray = new long[lenght];
        int i = 0;
        for (Long cursor : cursors) {
            cursorsArray[i++] = cursor;
        }
        asyncKillCursors(cursorsArray);
    }

    @Override
    public void asyncKillCursors(long[] cursors) throws
            MongoException {
        Request request = newRequest(null);
        RequestBaseMessage baseMessage = new RequestBaseMessage(null, 0, request.getRequestId());

        KillCursorsMessage message = new KillCursorsMessage(baseMessage, cursors.length, cursors);
        processor.killCursors(request, message);
    }

    @Override
    public <Arg, Result> Result execute(
            Command<? super Arg, Result> command,
            String database,
            boolean isSlaveOk,
            Arg arg) throws MongoException {
        Preconditions.checkState(!closed, "This client is closed");
        CommandRequest<Arg> request = new CommandRequest<Arg>(
                connection,
                requestCounter++,
                database,
                null,
                0,
                arg,
                isSlaveOk
        );
        CommandReply<Result> reply = processor.execute(command, request);
//        CommandReply<Result> reply = commandsExecutor.execute(command, request);
        if (reply.isOk()) {
            return reply.getResult();
        }
        throw reply.getErrorAsException();
    }

    @Override
    public boolean isRemote() {
        return false;
    }

    @Override
    public void asyncInsert(
            String database,
            String collection,
            boolean continueOnError,
            List<? extends BsonDocument> docsToInsert) throws MongoException {
        Preconditions.checkState(!closed, "This client is closed");

        Request newRequest = newRequest(database);
        RequestBaseMessage baseMessage = new RequestBaseMessage(null, 0, newRequest.getRequestId());
        EnumSet<InsertMessage.Flag> flags = EnumSet.noneOf(InsertMessage.Flag.class);
        if (continueOnError) {
            flags.add(Flag.CONTINUE_ON_ERROR);
        }
        InsertMessage message = new InsertMessage(baseMessage, flags, collection, docsToInsert);

        processor.insert(newRequest, message);
    }

    @Override
    public void asyncUpdate(
            String database,
            String collection,
            EnumSet<UpdateMessage.Flag> flags,
            BsonDocument selector,
            BsonDocument update) throws MongoException {
        Preconditions.checkState(!closed, "This client is closed");

        Request newRequest = newRequest(database);
        RequestBaseMessage baseMessage = new RequestBaseMessage(null, 0, newRequest.getRequestId());
        UpdateMessage message = new UpdateMessage(baseMessage, flags, collection, selector, update);

        processor.update(newRequest, message);
    }

    @Override
    public void asyncDelete(
            String database,
            String collection,
            EnumSet<DeleteMessage.Flag> flags,
            BsonDocument selector) throws MongoException {
        Preconditions.checkState(!closed, "This client is closed");

        Request newRequest = newRequest(database);
        RequestBaseMessage baseMessage = new RequestBaseMessage(null, 0, newRequest.getRequestId());
        DeleteMessage message = new DeleteMessage(baseMessage, flags, collection, selector);

        processor.delete(newRequest, message);
    }

    @Override
    public void close() {
        if (!closed) {
            processor.onConnectionInactive(connection);
            closed = true;
        }
    }

    public class MyCursor implements MongoCursor<BsonDocument> {
        private static final int DEFAULT_MAX_BATCH_SIZE = 1024;
        private final long cursorId;
        private final String database;
        private final String collection;
        private final boolean tailable;

        private List<BsonDocument> firstBatch;
        private int maxBatchSize;
        private boolean dead = false;

        public MyCursor(
                long cursorId,
                String database,
                String collection,
                boolean tailable,
                List<BsonDocument> firstBatch) {
            this.cursorId = cursorId;
            this.maxBatchSize = DEFAULT_MAX_BATCH_SIZE;
            this.database = database;
            this.collection = collection;
            this.tailable = tailable;
            this.firstBatch = firstBatch;
        }

        @Override
        public String getDatabase() {
            return database;
        }

        @Override
        public String getCollection() {
            return collection;
        }

        @Override
        public long getId() {
            return cursorId;
        }

        @Override
        public boolean isTailable() {
            return tailable;
        }

        @Override
        public boolean isDead() {
            return dead;
        }

        @Override
        public void setMaxBatchSize(int newBatchSize) {
            this.maxBatchSize = newBatchSize;
        }

        @Override
        public int getMaxBatchSize() {
            return maxBatchSize;
        }

        @Override
        public Batch<BsonDocument> fetchBatch() throws MongoException {
            if (closed) {
                throw new DeadCursorException();
            }

            long fetchTime = System.currentTimeMillis();

            if (firstBatch != null) {
                Batch<BsonDocument> result = new SimpleBatch<BsonDocument>(firstBatch, cursorId);
                firstBatch = null;
                return result;
            }
            Request request = newRequest(database);
            GetMoreMessage message = new GetMoreMessage(
                    newBaseMessage(request),
                    database + '.' + collection,
                    maxBatchSize,
                    cursorId
            );

            ReplyMessage more = processor.getMore(request, message);

            EnumSet<ReplyMessage.Flag> flags = more.getFlags();
            if (flags != null) {
                if (flags.contains(ReplyMessage.Flag.CURSOR_NOT_FOUND)) {
                    dead = true;
                    throw new CursorNotFoundException(more.getCursorId());
                }
                if (flags.contains(ReplyMessage.Flag.QUERY_FAILURE)) {
                    dead = true;
                    throw new UnknownErrorException("GetMore response: Query failure");
                }
            }
            ImmutableList<BsonDocument> documents = more.getDocuments();

            return new SimpleBatch<BsonDocument>(documents, fetchTime);
        }

        @Override
        public BsonDocument getOne() throws MongoException {
            Batch<BsonDocument> batch = fetchBatch();
            if (!batch.hasNext()) {
                return null;
            }
            BsonDocument result = batch.next();
            batch.close();
            close();
            return result;
        }

        @Override
        public void close() {
            if (dead) {
                return;
            }
            dead = true;
            if (closed) {
                return ;
            }
            Request request = newRequest(database);
            KillCursorsMessage message = new KillCursorsMessage(
                    newBaseMessage(request),
                    1,
                    new long[] {cursorId}
            );
            try {
                processor.killCursors(request, message);
            } catch (MongoException ex) {
                LOGGER.warn("Error while trying to close a cursor", ex);
            }
        }

        @Override
        public Iterator<BsonDocument> iterator() {
            return new MongoCursorIterator<BsonDocument>(this);
        }
    }
}
