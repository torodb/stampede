
package com.torodb.mongodb.commands.impl;

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandImplementation;
import com.eightkdata.mongowp.server.api.Request;
import com.torodb.mongodb.core.MongodConnection;

/**
 *
 */
public final class NotImplementedCommandImplementation<Arg, Result> implements
        CommandImplementation<Arg, Result, MongodConnection>{

    private static final NotImplementedCommandImplementation INSTANCE = new NotImplementedCommandImplementation();

    private NotImplementedCommandImplementation() {
    }

    public static <Arg, Rep> NotImplementedCommandImplementation<Arg, Rep> build() {
        return INSTANCE;
    }

    @Override
    public Status<Result> apply(Command<? super Arg, ? super Result> command, Arg arg, Request<MongodConnection> req) {
        return Status.from(ErrorCode.COMMAND_NOT_SUPPORTED, "Command not supported: " + command.getCommandName());
    }

}
