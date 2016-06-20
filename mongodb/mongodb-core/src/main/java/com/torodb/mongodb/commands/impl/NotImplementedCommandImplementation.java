
package com.torodb.mongodb.commands.impl;

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandImplementation;
import com.eightkdata.mongowp.server.api.Request;

/**
 *
 */
public final class NotImplementedCommandImplementation<Arg, Result, Context> implements
        CommandImplementation<Arg, Result, Context>{

    private static final NotImplementedCommandImplementation INSTANCE = new NotImplementedCommandImplementation();

    private NotImplementedCommandImplementation() {
    }

    public static <Arg, Rep, Context> NotImplementedCommandImplementation<Arg, Rep, Context> build() {
        return INSTANCE;
    }

    @Override
    public Status<Result> apply(Request req, Command<? super Arg, ? super Result> command, Arg arg, Context context) {
        return Status.from(ErrorCode.COMMAND_NOT_SUPPORTED, "Command not supported: " + command.getCommandName());
    }

}
