
package com.torodb.torod.mongodb.commands;

import com.eightkdata.mongowp.mongoserver.api.safe.*;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.CommandNotSupportedException;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.MongoException;

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
