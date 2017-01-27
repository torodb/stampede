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

package com.torodb.mongodb.commands.impl;

import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandExecutor;
import com.eightkdata.mongowp.server.api.CommandImplementation;
import com.eightkdata.mongowp.server.api.impl.MapBasedCommandExecutor;
import com.google.common.collect.ImmutableMap;
import com.torodb.core.BuildProperties;
import com.torodb.mongodb.commands.CmdImplMapSupplier;
import com.torodb.mongodb.commands.CommandClassifier;
import com.torodb.mongodb.commands.RequiredTransaction;
import com.torodb.mongodb.core.ExclusiveWriteMongodTransaction;
import com.torodb.mongodb.core.MongodConnection;
import com.torodb.mongodb.core.MongodServerConfig;
import com.torodb.mongodb.core.MongodTransaction;
import com.torodb.mongodb.core.ReadOnlyMongodTransaction;
import com.torodb.mongodb.core.WriteMongodTransaction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import javax.annotation.concurrent.ThreadSafe;

/**
 * An object that provides {@link CommandExecutor} classified by their usage.
 */
@ThreadSafe
public class CommandClassifierImpl implements CommandClassifier {
  private static final Logger LOGGER = LogManager.getLogger(CommandClassifierImpl.class);
  private final CommandExecutor<ExclusiveWriteMongodTransaction> exclusiveWriteCommandsExecutor;
  private final CommandExecutor<WriteMongodTransaction> writeCommandsExecutor;
  private final CommandExecutor<ReadOnlyMongodTransaction> readOnlyCommandsExecutor;
  private final CommandExecutor<MongodConnection> connectionCommandsExecutor;
  private final HashMap<Command<?, ?>, RequiredTransaction> requiredTranslationMap;

  @SuppressWarnings("checkstyle:LineLength")
  public CommandClassifierImpl(
      CmdImplMapSupplier<ExclusiveWriteMongodTransaction> exclusiveWriteTransImpls,
      CmdImplMapSupplier<WriteMongodTransaction> writeTransImpls,
      CmdImplMapSupplier<MongodTransaction> generalTransImpls,
      CmdImplMapSupplier<MongodConnection> connTransImpls) {

    this.requiredTranslationMap = classifyCommands(exclusiveWriteTransImpls, writeTransImpls,
        generalTransImpls, connTransImpls);

    ImmutableMap.Builder<Command<?, ?>, CommandImplementation<?, ?, ? super ExclusiveWriteMongodTransaction>> exclusiveWriteMapBuilder =
        ImmutableMap.builder();
    ImmutableMap.Builder<Command<?, ?>, CommandImplementation<?, ?, ? super WriteMongodTransaction>> writeMapBuilder =
        ImmutableMap.builder();
    ImmutableMap.Builder<Command<?, ?>, CommandImplementation<?, ?, ? super ReadOnlyMongodTransaction>> readOnlyMapBuilder =
        ImmutableMap.builder();

    generalTransImpls.get().entrySet().stream()
        .filter(CommandClassifierImpl::isImplemented)
        .forEach((entry) -> {
          exclusiveWriteMapBuilder.put(entry);
          writeMapBuilder.put(entry);
          readOnlyMapBuilder.put(entry);
        });

    writeTransImpls.get().entrySet().stream()
        .filter(CommandClassifierImpl::isImplemented)
        .forEach(entry -> {
          writeMapBuilder.put(entry);
          exclusiveWriteMapBuilder.put(entry);
        });

    exclusiveWriteTransImpls.get().entrySet().stream()
        .filter(CommandClassifierImpl::isImplemented)
        .forEach(entry -> exclusiveWriteMapBuilder.put(entry));

    this.exclusiveWriteCommandsExecutor = MapBasedCommandExecutor.fromMap(exclusiveWriteMapBuilder
        .build());
    this.writeCommandsExecutor = MapBasedCommandExecutor.fromMap(writeMapBuilder.build());
    this.readOnlyCommandsExecutor = MapBasedCommandExecutor.fromMap(readOnlyMapBuilder.build());
    this.connectionCommandsExecutor = MapBasedCommandExecutor.fromMap(connTransImpls.get()
    );
  }

  /**
   * Creates a default {@link CommandClassifier}.
   */
  public static CommandClassifierImpl createDefault(Clock clock, BuildProperties buildProp,
      MongodServerConfig mongodServerConfig) {
    return new CommandClassifierImpl(
        new ExclusiveWriteTransactionCmdsImpl(),
        new WriteTransactionCmdImpl(),
        new GeneralTransactionCmdImpl(),
        new ConnectionCmdImpl(clock, buildProp, mongodServerConfig)
    );
  }

  private static HashMap<Command<?, ?>, RequiredTransaction> classifyCommands(
      CmdImplMapSupplier<ExclusiveWriteMongodTransaction> exclusiveWriteTransImpls,
      CmdImplMapSupplier<WriteMongodTransaction> writeTransImpls,
      CmdImplMapSupplier<MongodTransaction> generalTransImpls,
      CmdImplMapSupplier<MongodConnection> connTransImpls) {

    HashMap<Command<?, ?>, RequiredTransaction> map = new HashMap<>();

    exclusiveWriteTransImpls.get().keySet().stream()
        .forEach(cmd -> classifyCommand(map, cmd, RequiredTransaction.EXCLUSIVE_WRITE_TRANSACTION));
    writeTransImpls.get().keySet().stream()
        .forEach(cmd -> classifyCommand(map, cmd, RequiredTransaction.WRITE_TRANSACTION));
    generalTransImpls.get().keySet().stream()
        .forEach(cmd -> classifyCommand(map, cmd, RequiredTransaction.READ_TRANSACTION));
    connTransImpls.get().keySet().stream()
        .forEach(cmd -> classifyCommand(map, cmd, RequiredTransaction.NO_TRANSACTION));

    return map;
  }

  private static void classifyCommand(HashMap<Command<?, ?>, RequiredTransaction> map,
      Command<?, ?> c, RequiredTransaction rt) {
    RequiredTransaction oldRequiredTrans = map.put(c, rt);
    if (oldRequiredTrans != null) {
      LOGGER.warn("The command {} is classified as it requires {} but also {}", c, rt,
          oldRequiredTrans);
    }
  }

  @SuppressWarnings("checkstyle:LineLength")
  @Override
  public CommandExecutor<? super ExclusiveWriteMongodTransaction> getExclusiveWriteCommandsExecutor() {
    return exclusiveWriteCommandsExecutor;
  }

  @Override
  public CommandExecutor<? super WriteMongodTransaction> getWriteCommandsExecutor() {
    return writeCommandsExecutor;
  }

  @Override
  public CommandExecutor<? super ReadOnlyMongodTransaction> getReadOnlyCommandsExecutor() {
    return readOnlyCommandsExecutor;
  }

  @Override
  public CommandExecutor<? super MongodConnection> getConnectionCommandsExecutor() {
    return connectionCommandsExecutor;
  }

  @Override
  public RequiredTransaction classify(Command<?, ?> command) {
    return requiredTranslationMap.get(command);
  }

  @Override
  public Stream<Command<?, ?>> streamAllCommands() {
    return requiredTranslationMap.keySet().stream();
  }

  private static <E> boolean isImplemented(
      Map.Entry<Command<?, ?>, CommandImplementation<?, ?, ? super E>> entry) {
    return !(entry.getValue() instanceof NotImplementedCommandImplementation);
  }

}
