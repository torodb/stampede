
package com.torodb.torod.mongodb.standard.commands;

import com.eightkdata.mongowp.mongoserver.api.safe.*;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.CommandNotSupportedException;

/**
 *
 */
public class NotImplementedCommandImplementation<Arg extends CommandArgument, Rep extends CommandReply> implements
        CommandImplementation<Arg, Rep>{

    private static final NotImplementedCommandImplementation INSTANCE = new NotImplementedCommandImplementation();

    private NotImplementedCommandImplementation() {
    }

    public static <Arg extends CommandArgument, Rep extends CommandReply> NotImplementedCommandImplementation<Arg, Rep> build() {
        return INSTANCE;
    }

    @Override
    public Rep apply(Command<? extends Arg, ? extends Rep> command, CommandRequest<Arg> req) throws CommandNotSupportedException {
        throw new CommandNotSupportedException(command.getCommandName());
    }

}
