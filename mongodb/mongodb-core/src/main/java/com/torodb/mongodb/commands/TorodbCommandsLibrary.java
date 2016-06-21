
package com.torodb.mongodb.commands;

import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandsLibrary;
import com.eightkdata.mongowp.server.api.impl.NameBasedCommandsLibrary;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
public class TorodbCommandsLibrary implements CommandsLibrary {

    private static final Logger LOGGER = LogManager.getLogger(TorodbCommandsLibrary.class);
    private final CommandsLibrary allCommands;
    private final CommandsLibrary noTransactionsLibrary, readLibrary, writeLibrary;
    private final Map<Command<?,?>, RequiredTransaction> requiredTranslationMap;

    @Inject
    public TorodbCommandsLibrary(ConnectionCommandsExecutor connectionExecutor,
            ReadOnlyTransactionCommandsExecutor readOnlyExecutor,
            WriteTransactionCommandsExecutor writeExecutor) {
        String version = "torodb-3.2-like";

        this.allCommands = new NameBasedCommandsLibrary(version,
                Iterables.concat(
                        connectionExecutor.getSupportedCommands(),
                        readOnlyExecutor.getSupportedCommands(),
                        writeExecutor.getSupportedCommands()
                )
        );
        
        requiredTranslationMap = new HashMap<>();
        this.writeLibrary = new NameBasedCommandsLibrary(version, writeExecutor.getSupportedCommands());
        writeLibrary.getSupportedCommands().forEach((c) -> {
            classifyCommand(c, RequiredTransaction.WRITE_TRANSACTION);
        });
        this.readLibrary = new NameBasedCommandsLibrary(version, readOnlyExecutor.getSupportedCommands());
        readLibrary.getSupportedCommands().forEach((c) -> {
            classifyCommand(c, RequiredTransaction.READ_TRANSACTION);
        });
        this.noTransactionsLibrary = new NameBasedCommandsLibrary(version, connectionExecutor.getSupportedCommands());
        noTransactionsLibrary.getSupportedCommands().forEach((c) -> {
            classifyCommand(c, RequiredTransaction.NO_TRANSACTION);
        });
    }

    public RequiredTransaction getCommandType(Command<?,?> command) {
        RequiredTransaction requiredTransaction = requiredTranslationMap.get(command);
        Preconditions.checkArgument(requiredTransaction != null ,
                "It is not registered which transaction is required to execute " + command);
        return requiredTransaction;
    }

    @Override
    public String getSupportedVersion() {
        return allCommands.getSupportedVersion();
    }

    @Override
    public Set<Command> getSupportedCommands() {
        return allCommands.getSupportedCommands();
    }

    @Override
    public Command find(BsonDocument requestDocument) {
        return allCommands.find(requestDocument);
    }

    CommandsLibrary getConnectionLibary() {
        return noTransactionsLibrary;
    }

    CommandsLibrary getWriteTransactionLibary() {
        return writeLibrary;
    }

    CommandsLibrary getReadTransactionLibary() {
        return readLibrary;
    }

    private void classifyCommand(Command<?,?> c, @Nonnull RequiredTransaction rt) {
        RequiredTransaction oldRT = requiredTranslationMap.put(c, rt);
        if (oldRT != null) {
            LOGGER.warn("The command {} is classified as it requires {} but also {}", c, rt, oldRT);
        }
    }

    public static enum RequiredTransaction {
        NO_TRANSACTION,
        READ_TRANSACTION,
        WRITE_TRANSACTION
    }

}
