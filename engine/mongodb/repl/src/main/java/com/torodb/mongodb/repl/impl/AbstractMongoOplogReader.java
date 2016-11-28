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

package com.torodb.mongodb.repl.impl;

import static com.eightkdata.mongowp.bson.utils.DefaultBsonValues.newInt;

import com.eightkdata.mongowp.OpTime;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.utils.DefaultBsonValues;
import com.eightkdata.mongowp.client.core.MongoConnection;
import com.eightkdata.mongowp.exceptions.BadValueException;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.exceptions.NoSuchKeyException;
import com.eightkdata.mongowp.exceptions.OplogOperationUnsupported;
import com.eightkdata.mongowp.exceptions.OplogStartMissingException;
import com.eightkdata.mongowp.exceptions.TypesMismatchException;
import com.eightkdata.mongowp.messages.request.QueryMessage.QueryOption;
import com.eightkdata.mongowp.messages.request.QueryMessage.QueryOptions;
import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.eightkdata.mongowp.server.api.pojos.MongoCursor;
import com.eightkdata.mongowp.server.api.pojos.MongoCursor.Batch;
import com.eightkdata.mongowp.server.api.pojos.MongoCursor.DeadCursorException;
import com.eightkdata.mongowp.server.api.pojos.TransformationMongoCursor;
import com.eightkdata.mongowp.utils.BsonArrayBuilder;
import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;
import com.torodb.mongodb.commands.pojos.OplogOperationParser;
import com.torodb.mongodb.repl.OplogReader;

import java.util.EnumSet;
import java.util.function.Consumer;

public abstract class AbstractMongoOplogReader implements OplogReader {

  private static final String DATABASE = "local";
  private static final String COLLECTION = "oplog.rs";

  private static final BsonDocument NATURAL_ORDER_SORT 
      = DefaultBsonValues.newDocument("$natural", newInt(1));
  private static final BsonDocument INVERSE_ORDER_SORT 
      = DefaultBsonValues.newDocument("$natural", newInt(-1));

  protected abstract MongoConnection consumeConnection();

  protected abstract void releaseConnection(MongoConnection connection);

  @Override
  public MongoCursor<OplogOperation> queryGte(OpTime lastFetchedOpTime) throws MongoException {
    BsonDocument query = DefaultBsonValues.newDocument(
        "ts",
        DefaultBsonValues.newDocument("$gte", lastFetchedOpTime.getTimestamp())
    );

    EnumSet<QueryOption> flags = EnumSet.of(
        QueryOption.AWAIT_DATA,
        QueryOption.TAILABLE_CURSOR
    );

    return query(query, flags, NATURAL_ORDER_SORT);
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
        DefaultBsonValues.newDocument(
            "ts",
            DefaultBsonValues.newDocument(includeFrom ? "$gte" : "$gt", from.getTimestamp())
        )
    );
    conditions.add(
        DefaultBsonValues.newDocument(
            "ts",
            DefaultBsonValues.newDocument(includeTo ? "$lte" : "$lt", to.getTimestamp())
        )
    );

    EnumSet<QueryOption> flags = EnumSet.noneOf(QueryOption.class);

    return query(
        DefaultBsonValues.newDocument("$and", conditions.build()),
        flags,
        NATURAL_ORDER_SORT);
  }

  public MongoCursor<OplogOperation> query(BsonDocument query, EnumSet<QueryOption> flags,
      BsonDocument sortBy) throws MongoException {
    Preconditions.checkState(!isClosed(), "You have to connect this client before");

    MongoConnection connection = consumeConnection();
    MongoCursor<BsonDocument> cursor = connection.query(
        DATABASE,
        COLLECTION,
        query,
        0,
        0,
        new QueryOptions(flags),
        sortBy,
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

    BsonDocument query = DefaultBsonValues.EMPTY_DOC;
    BsonDocument orderBy = first ? NATURAL_ORDER_SORT : INVERSE_ORDER_SORT;

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
          orderBy,
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
      } catch (BadValueException | TypesMismatchException | NoSuchKeyException ex) {
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
    public Batch<T> fetchBatch() throws MongoException,
        DeadCursorException {
      return delegate.fetchBatch();
    }

    @Override
    public T next() {
      return delegate.next();
    }

    @Override
    public HostAndPort getServerAddress() {
      return delegate.getServerAddress();
    }

    @Override
    public boolean hasNext() {
      return delegate.hasNext();
    }

    @Override
    public T tryNext() {
      return delegate.tryNext();
    }

    @Override
    public void remove() {
      delegate.remove();
    }

    @Override
    public void forEachRemaining(Consumer<? super T> action) {
      delegate.forEachRemaining(action);
    }

    @Override
    public Batch<T> tryFetchBatch() throws MongoException, DeadCursorException {
      return delegate.tryFetchBatch();
    }

    @Override
    public boolean isClosed() {
      return delegate.isClosed();
    }

    @Override
    public void close() {
      delegate.close();
      releaseConnection(connection);
    }
  }
}
