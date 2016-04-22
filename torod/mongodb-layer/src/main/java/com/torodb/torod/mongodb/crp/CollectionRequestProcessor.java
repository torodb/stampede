
package com.torodb.torod.mongodb.crp;

import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.messages.request.DeleteMessage;
import com.eightkdata.mongowp.messages.request.InsertMessage;
import com.eightkdata.mongowp.messages.request.UpdateMessage;
import com.eightkdata.mongowp.messages.utils.IterableDocumentProvider;
import com.eightkdata.mongowp.server.api.Request;
import com.eightkdata.mongowp.server.api.impl.UpdateOpResult;
import com.eightkdata.mongowp.server.api.pojos.QueryRequest;
import com.eightkdata.mongowp.server.callback.WriteOpResult;
import com.google.common.util.concurrent.ListenableFuture;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

/**
 *
 */
public interface CollectionRequestProcessor {

    @Nonnull
    public QueryResponse query(Request request, QueryRequest queryMessage)
            throws MongoException;

    public ListenableFuture<? extends WriteOpResult> insert(Request request, InsertMessage insertMessage)
            throws MongoException;

    public ListenableFuture<? extends UpdateOpResult> update(Request request, UpdateMessage updateMessage)
            throws MongoException;

    public ListenableFuture<? extends WriteOpResult> delete(Request request, DeleteMessage deleteMessage)
            throws MongoException;

    public static class QueryResponse {
        @Nonnegative
        private final long cursorId;
        @Nonnull
        final private IterableDocumentProvider<BsonDocument> documents;

        public QueryResponse(long cursorId, IterableDocumentProvider<BsonDocument> documents) {
            this.cursorId = cursorId;
            this.documents = documents;
        }

        public QueryResponse(long cursorId, Iterable<BsonDocument> documents) {
            this.cursorId = cursorId;
            this.documents = IterableDocumentProvider.of(documents);
        }

        public long getCursorId() {
            return cursorId;
        }

        public IterableDocumentProvider<BsonDocument> getDocuments() {
            return documents;
        }
    }
}
