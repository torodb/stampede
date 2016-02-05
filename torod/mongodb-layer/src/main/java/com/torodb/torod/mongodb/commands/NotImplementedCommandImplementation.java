
package com.torodb.torod.mongodb.commands;

import com.eightkdata.mongowp.exceptions.CommandNotSupportedException;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandImplementation;
import com.eightkdata.mongowp.server.api.CommandRequest;
import com.eightkdata.mongowp.server.api.CommandResult;

/**
 *
 */
public final class NotImplementedCommandImplementation<Arg, Result> implements
        CommandImplementation<Arg, Result>{

    private static final NotImplementedCommandImplementation INSTANCE = new NotImplementedCommandImplementation();

    private NotImplementedCommandImplementation() {
    }

    public static <Arg, Rep> NotImplementedCommandImplementation<Arg, Rep> build() {
        return INSTANCE;
    }

    @Override
    public CommandResult<Result> apply(Command<? super Arg, ? super Result> command, CommandRequest<Arg> req)
            throws MongoException {
        throw new CommandNotSupportedException(command.getCommandName());
    }

}
