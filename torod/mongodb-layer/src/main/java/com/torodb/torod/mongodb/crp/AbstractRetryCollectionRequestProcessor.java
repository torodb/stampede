
package com.torodb.torod.mongodb.crp;

import java.util.concurrent.Callable;

import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.messages.request.DeleteMessage;
import com.eightkdata.mongowp.messages.request.InsertMessage;
import com.eightkdata.mongowp.messages.request.UpdateMessage;
import com.eightkdata.mongowp.server.api.Request;
import com.eightkdata.mongowp.server.api.impl.UpdateOpResult;
import com.eightkdata.mongowp.server.callback.WriteOpResult;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.torodb.common.util.RetryHelper;
import com.torodb.common.util.RetryHelper.DelegateExceptionHandler;
import com.torodb.common.util.RetryHelper.ExceptionHandler;
import com.torodb.common.util.RetryHelper.RetryCallback;
import com.torodb.torod.mongodb.RetryException;

/**
 *
 */
public abstract class AbstractRetryCollectionRequestProcessor implements CollectionRequestProcessor {

    private final RetryTransactionHandler<ListenableFuture<? extends WriteOpResult>> writeRetryTransactionHandler = 
            new RetryTransactionHandler<ListenableFuture<? extends WriteOpResult>>(
                    RetryHelper.<ListenableFuture<? extends WriteOpResult>, MongoException>retryUntilHandler(64, 
                            RetryHelper.<ListenableFuture<? extends WriteOpResult>, MongoException>throwHandler()));
    
    private final RetryTransactionHandler<ListenableFuture<? extends UpdateOpResult>> updateRetryTransactionHandler = 
            new RetryTransactionHandler<ListenableFuture<? extends UpdateOpResult>>(
                    RetryHelper.<ListenableFuture<? extends UpdateOpResult>, MongoException>retryUntilHandler(64, 
                            RetryHelper.<ListenableFuture<? extends UpdateOpResult>, MongoException>throwHandler()));
    
    @Override
    public final ListenableFuture<? extends WriteOpResult> insert(Request request, InsertMessage insertMessage)
            throws MongoException {
        return RetryHelper.retryOrThrow(writeRetryTransactionHandler, new TryInsert(request, insertMessage));
    }
    
    protected abstract ListenableFuture<? extends WriteOpResult> tryInsert(Request request, InsertMessage insertMessage) 
            throws MongoException;
    
    @Override
    public final ListenableFuture<? extends UpdateOpResult> update(Request request, UpdateMessage updateMessage)
            throws MongoException {
        return RetryHelper.retryOrThrow(updateRetryTransactionHandler, new TryUpdate(request, updateMessage));
    }
    
    protected abstract ListenableFuture<? extends UpdateOpResult> tryUpdate(Request request, UpdateMessage updateMessage) 
            throws MongoException;
    
    @Override
    public final ListenableFuture<? extends WriteOpResult> delete(Request request, DeleteMessage deleteMessage)
            throws MongoException {
        return RetryHelper.retryOrThrow(writeRetryTransactionHandler, new TryDelete(request, deleteMessage));
    }
    
    protected abstract ListenableFuture<? extends WriteOpResult> tryDelete(Request request, DeleteMessage deleteMessage)
            throws MongoException;
    
    private class TryInsert implements Callable<ListenableFuture<? extends WriteOpResult>> {
        private final Request request;
        private final InsertMessage insertMessage;

        public TryInsert(Request request, InsertMessage insertMessage) {
            super();
            this.request = request;
            this.insertMessage = insertMessage;
        }

        @Override
        public ListenableFuture<? extends WriteOpResult> call() throws Exception {
            return tryInsert(request, insertMessage);
        }
    }
    
    private class TryUpdate implements Callable<ListenableFuture<? extends UpdateOpResult>> {
        private final Request request;
        private final UpdateMessage updateMessage;

        public TryUpdate(Request request, UpdateMessage updateMessage) {
            super();
            this.request = request;
            this.updateMessage = updateMessage;
        }

        @Override
        public ListenableFuture<? extends UpdateOpResult> call() throws Exception {
            return tryUpdate(request, updateMessage);
        }
    }
    
    private class TryDelete implements Callable<ListenableFuture<? extends WriteOpResult>> {
        private final Request request;
        private final DeleteMessage deleteMessage;

        public TryDelete(Request request, DeleteMessage deleteMessage) {
            super();
            this.request = request;
            this.deleteMessage = deleteMessage;
        }

        @Override
        public ListenableFuture<? extends WriteOpResult> call() throws Exception {
            return tryDelete(request, deleteMessage);
        }
    }
    
    private class RetryTransactionHandler<Result> extends DelegateExceptionHandler<Result, MongoException> {
        public RetryTransactionHandler(ExceptionHandler<Result, MongoException> delegate) {
            super(delegate);
        }

        @Override
        public void handleException(RetryCallback<Result> callback, Exception t, int attempts) throws MongoException {
            Preconditions.checkArgument(t instanceof MongoException);
            
            if (t instanceof RetryException) {
                super.handleException(callback, t, attempts);
                return;
            }
            
            throw (MongoException) t;
        }
    }
}
