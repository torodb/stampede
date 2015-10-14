
package com.torodb.torod.mongodb.crp;

import com.eightkdata.mongowp.messages.request.DeleteMessage;
import com.eightkdata.mongowp.messages.request.InsertMessage;
import com.eightkdata.mongowp.messages.request.UpdateMessage;
import com.eightkdata.mongowp.mongoserver.api.safe.Request;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.UpdateOpResult;
import com.eightkdata.mongowp.mongoserver.api.safe.pojos.QueryRequest;
import com.eightkdata.mongowp.mongoserver.callback.WriteOpResult;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.MongoException;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.List;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import org.bson.BsonDocument;

/**
 *
 */
public interface CollectionRequestProcessor {

    @Nonnull
    public QueryResponse query(Request request, QueryRequest queryMessage)
            throws MongoException;

    public ListenableFuture<? extends WriteOpResult> insert(Request request, InsertMessage insertMessage)
            throws MongoException;

    public ListenableFuture<? extends UpdateOpResult> update(Request request, UpdateMessage deleteMessage)
            throws MongoException;

    public ListenableFuture<? extends WriteOpResult> delete(Request request, DeleteMessage deleteMessage)
            throws MongoException;

    public static class QueryResponse {
        @Nonnegative
        private final long cursorId;
        @Nonnull
        final private ImmutableList<BsonDocument> documents;

        public QueryResponse(long cursorId, List<BsonDocument> documents) {
            this.cursorId = cursorId;
            this.documents = ImmutableList.copyOf(documents);
        }

        public long getCursorId() {
            return cursorId;
        }

        public ImmutableList<BsonDocument> getDocuments() {
            return documents;
        }
    }
}
