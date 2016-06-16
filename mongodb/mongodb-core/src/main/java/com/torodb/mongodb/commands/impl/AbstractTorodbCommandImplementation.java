package com.torodb.mongodb.commands.impl;

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.server.api.CommandImplementation;
import com.torodb.mongodb.core.MongodConnection;
import com.torodb.mongodb.core.Retrier;
import java.util.concurrent.Callable;

/**
 *
 */
public abstract class AbstractTorodbCommandImplementation<Arg, Result> implements CommandImplementation<Arg, Result, MongodConnection>{

    private final String commandName;
    private final Retrier retrier;

    public AbstractTorodbCommandImplementation(String commandName, Retrier retrier) {
        this.commandName = commandName;
        this.retrier = retrier;
    }

    /**
     *
     * @param callable it cannot return null
     * @return
     */
    public Status<Result> retry(Callable<Result> callable) {
        Result retryResult = retrier.retry(callable, (Result) null);
        if (retryResult == null) {
            return Status.from(
                    ErrorCode.CONFLICTING_OPERATION_IN_PROGRESS,
                    "It was impossible to execute " + commandName + " after several attempts"
            );
        }
        else {
            return Status.ok(retryResult);
        }
    }
}
