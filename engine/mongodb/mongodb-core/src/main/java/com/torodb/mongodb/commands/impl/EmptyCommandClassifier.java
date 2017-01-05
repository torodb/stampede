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
import com.eightkdata.mongowp.server.api.impl.MapBasedCommandExecutor;
import com.google.common.collect.ImmutableMap;
import com.torodb.mongodb.commands.CommandClassifier;
import com.torodb.mongodb.commands.RequiredTransaction;
import com.torodb.mongodb.core.ExclusiveWriteMongodTransaction;
import com.torodb.mongodb.core.MongodConnection;
import com.torodb.mongodb.core.ReadOnlyMongodTransaction;
import com.torodb.mongodb.core.WriteMongodTransaction;

import java.util.stream.Stream;

import com.eightkdata.mongowp.server.api.CommandExecutor;

/**
 *
 */
public class EmptyCommandClassifier implements CommandClassifier {

  @SuppressWarnings("checkstyle:LineLength")
  @Override
  public CommandExecutor<? super ExclusiveWriteMongodTransaction> getExclusiveWriteCommandsExecutor() {
    return MapBasedCommandExecutor.fromMap(ImmutableMap.of());
  }

  @Override
  public CommandExecutor<? super WriteMongodTransaction> getWriteCommandsExecutor() {
    return MapBasedCommandExecutor.fromMap(ImmutableMap.of());
  }

  @Override
  public CommandExecutor<? super ReadOnlyMongodTransaction> getReadOnlyCommandsExecutor() {
    return MapBasedCommandExecutor.fromMap(ImmutableMap.of());
  }

  @Override
  public CommandExecutor<? super MongodConnection> getConnectionCommandsExecutor() {
    return MapBasedCommandExecutor.fromMap(ImmutableMap.of());
  }

  @Override
  public RequiredTransaction classify(Command<?, ?> command) {
    return RequiredTransaction.NO_TRANSACTION;
  }

  @Override
  public Stream<Command<?, ?>> streamAllCommands() {
    return Stream.empty();
  }

}
