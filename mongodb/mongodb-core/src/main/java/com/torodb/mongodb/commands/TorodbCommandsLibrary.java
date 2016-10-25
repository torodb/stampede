
package com.torodb.mongodb.commands;

import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandsLibrary;
import com.eightkdata.mongowp.server.api.impl.NameBasedCommandsLibrary;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
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
    private final CommandsLibrary noTransactionsLibrary, readLibrary, writeLibrary, exclusiveWriteLibrary;
    private final Map<Command<?,?>, RequiredTransaction> requiredTranslationMap;

    @Inject
    public TorodbCommandsLibrary(ConnectionCommandsExecutor connectionExecutor,
            GeneralTransactionImplementations readOnlyExecutor,
            WriteTransactionImplementations writeExecutor,
            ExclusiveWriteTransactionImplementations exclusiveWriteExecutor) {
        String version = "torodb-3.2-like";
        
        requiredTranslationMap = new HashMap<>();

        Function<Iterable<Command<?,?>>, CommandsLibrary> libraryFactory = (it) -> {
            return new NameBasedCommandsLibrary.Builder(version)
                .addCommands(it)
                .build();
        };

        this.exclusiveWriteLibrary = libraryFactory.apply(
                exclusiveWriteExecutor.getSupportedCommands());
        exclusiveWriteLibrary.getSupportedCommands().forEach((c) -> {
            classifyCommand(c, RequiredTransaction.EXCLUSIVE_WRITE_TRANSACTION);
        });
        this.writeLibrary = libraryFactory.apply(
                writeExecutor.getSupportedCommands());
        writeLibrary.getSupportedCommands().forEach((c) -> {
            classifyCommand(c, RequiredTransaction.WRITE_TRANSACTION);
        });
        this.readLibrary = libraryFactory.apply(
                readOnlyExecutor.getSupportedCommands());
        readLibrary.getSupportedCommands().forEach((c) -> {
            classifyCommand(c, RequiredTransaction.READ_TRANSACTION);
        });
        this.noTransactionsLibrary = libraryFactory.apply(
                connectionExecutor.getSupportedCommands());
        noTransactionsLibrary.getSupportedCommands().forEach((c) -> {
            classifyCommand(c, RequiredTransaction.NO_TRANSACTION);
        });

        allCommands = libraryFactory.apply(
                requiredTranslationMap.keySet());
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
    public LibraryEntry find(BsonDocument requestDocument) {
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
        WRITE_TRANSACTION,
        EXCLUSIVE_WRITE_TRANSACTION
    }

}
