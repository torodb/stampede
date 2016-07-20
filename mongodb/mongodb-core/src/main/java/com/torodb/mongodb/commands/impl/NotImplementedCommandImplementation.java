
package com.torodb.mongodb.commands.impl;

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandImplementation;
import com.eightkdata.mongowp.server.api.Request;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
public final class NotImplementedCommandImplementation<Arg, Result, Context> implements
        CommandImplementation<Arg, Result, Context>{

    private static final Logger LOGGER
            = LogManager.getLogger(NotImplementedCommandImplementation.class);
    private static final NotImplementedCommandImplementation INSTANCE = new NotImplementedCommandImplementation();

    private NotImplementedCommandImplementation() {
    }

    public static <Arg, Rep, Context> NotImplementedCommandImplementation<Arg, Rep, Context> build() {
        return INSTANCE;
    }

    @Override
    public Status<Result> apply(Request req, Command<? super Arg, ? super Result> command, Arg arg, Context context) {
        LOGGER.warn("Command {} was called, but it is not supported", command.getCommandName());
        return Status.from(ErrorCode.COMMAND_NOT_SUPPORTED, "Command not supported: " + command.getCommandName());
    }

}
