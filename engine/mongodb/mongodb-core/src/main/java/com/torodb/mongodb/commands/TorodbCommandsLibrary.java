/*
 * ToroDB
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.mongodb.commands;

import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandsLibrary;
import com.eightkdata.mongowp.server.api.impl.NameBasedCommandsLibrary;
import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.inject.Inject;

/**
 *
 */
public class TorodbCommandsLibrary implements CommandsLibrary {

  private static final Logger LOGGER = LogManager.getLogger(TorodbCommandsLibrary.class);
  private final CommandsLibrary allCommands;
  private final CommandsLibrary noTransactionsLibrary;
  private final CommandsLibrary readLibrary;
  private final CommandsLibrary writeLibrary;
  private final CommandsLibrary exclusiveWriteLibrary;
  private final Map<Command<?, ?>, RequiredTransaction> requiredTranslationMap;

  @Inject
  public TorodbCommandsLibrary(ConnectionCommandsExecutor connectionExecutor,
      GeneralTransactionImplementations readOnlyExecutor,
      WriteTransactionImplementations writeExecutor,
      ExclusiveWriteTransactionImplementations exclusiveWriteExecutor) {
    String version = "torodb-3.2-like";

    requiredTranslationMap = new HashMap<>();

    Function<Iterable<Command<?, ?>>, CommandsLibrary> libraryFactory = (it) -> {
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

  public RequiredTransaction getCommandType(Command<?, ?> command) {
    RequiredTransaction requiredTransaction = requiredTranslationMap.get(command);
    Preconditions.checkArgument(requiredTransaction != null,
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

  private void classifyCommand(Command<?, ?> c, @Nonnull RequiredTransaction rt) {
    RequiredTransaction oldRequiredTrans = requiredTranslationMap.put(c, rt);
    if (oldRequiredTrans != null) {
      LOGGER.warn("The command {} is classified as it requires {} but also {}", c, rt, 
          oldRequiredTrans);
    }
  }

  public static enum RequiredTransaction {
    NO_TRANSACTION,
    READ_TRANSACTION,
    WRITE_TRANSACTION,
    EXCLUSIVE_WRITE_TRANSACTION
  }

}
