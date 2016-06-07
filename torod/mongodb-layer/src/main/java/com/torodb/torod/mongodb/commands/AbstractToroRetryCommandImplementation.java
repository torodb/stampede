
package com.torodb.torod.mongodb.commands;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.exceptions.UnknownErrorException;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandRequest;
import com.eightkdata.mongowp.server.api.CommandResult;
import com.google.common.base.Preconditions;
import com.torodb.common.util.RetryHelper;
import com.torodb.common.util.RetryHelper.DelegateExceptionHandler;
import com.torodb.common.util.RetryHelper.ExceptionHandler;
import com.torodb.common.util.RetryHelper.RetryCallback;
import com.torodb.torod.core.connection.exceptions.RetryTransactionException;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 *
 */
public abstract class AbstractToroRetryCommandImplementation<Arg, Rep> extends AbstractToroCommandImplementation<Arg, Rep> {
    
    private static final Logger LOGGER
            = LoggerFactory.getLogger(AbstractToroRetryCommandImplementation.class);

    private final RetryTransactionHandler<CommandResult<Rep>> retryTransactionHandler =
            new RetryTransactionHandler<>(
                    RetryHelper.<CommandResult<Rep>, MongoException>retryUntilHandler(64,
                            RetryHelper.<CommandResult<Rep>, MongoException>throwHandler()));
    
    @Nonnull
    @Override
    public final CommandResult<Rep> apply(Command<? super Arg, ? super Rep> command, CommandRequest<Arg> req) throws MongoException {
        return RetryHelper.retryOrThrow(retryTransactionHandler, new TryApplyer(command, req));
    }

    protected abstract CommandResult<Rep> tryApply(Command<? super Arg, ? super Rep> command, CommandRequest<Arg> req) throws MongoException;
    
    private class TryApplyer implements Callable<CommandResult<Rep>> {
        private final Command<? super Arg, ? super Rep> command;
        private final CommandRequest<Arg> req;

        public TryApplyer(Command<? super Arg, ? super Rep> command, CommandRequest<Arg> req) {
            super();
            this.command = command;
            this.req = req;
        }

        @Override
        public CommandResult<Rep> call() throws Exception {
            return tryApply(command, req);
        }
    }
    
    private class RetryTransactionHandler<Result> extends DelegateExceptionHandler<Result, MongoException> {
        public RetryTransactionHandler(ExceptionHandler<Result, MongoException> delegate) {
            super(delegate);
        }

        @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST",
                justification="The cast is enforced by a precondition")
        @Override
        public void handleException(RetryCallback<Result> callback, Exception t, int attempts) throws MongoException {
            if (!(t instanceof MongoException)) {
                throw new IllegalArgumentException(t);
            }
            
            boolean isRetryException = false;
            String retryExceptionMessage = null;
            if (t instanceof UnknownErrorException &&
                    t.getCause() instanceof ExecutionException &&
                    t.getCause().getCause() instanceof RetryTransactionException) {
                isRetryException = true;
                retryExceptionMessage = t.getCause().getCause().getMessage();
            } else
            if (t instanceof UnknownErrorException &&
                    t.getCause() instanceof RetryTransactionException) {
                isRetryException = true;
                retryExceptionMessage = t.getCause().getMessage();
            }
            
            if (isRetryException) {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Retrying operation due to exception: " + retryExceptionMessage);
                }
                super.handleException(callback, t, attempts);
                return;
            }
            
            throw (MongoException) t;
        }
    }
}
