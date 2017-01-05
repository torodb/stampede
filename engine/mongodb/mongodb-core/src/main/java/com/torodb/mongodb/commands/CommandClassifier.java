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

import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandExecutor;
import com.torodb.mongodb.core.ExclusiveWriteMongodTransaction;
import com.torodb.mongodb.core.MongodConnection;
import com.torodb.mongodb.core.ReadOnlyMongodTransaction;
import com.torodb.mongodb.core.WriteMongodTransaction;

import java.util.stream.Stream;

import javax.annotation.concurrent.ThreadSafe;

/**
 * An object that classifies a given command by the context it requires to execute and provides
 * a {@link CommandExecutor} for each context.
 */
@ThreadSafe
public interface CommandClassifier {

  CommandExecutor<? super ExclusiveWriteMongodTransaction> getExclusiveWriteCommandsExecutor();

  CommandExecutor<? super WriteMongodTransaction> getWriteCommandsExecutor();

  CommandExecutor<? super ReadOnlyMongodTransaction> getReadOnlyCommandsExecutor();

  CommandExecutor<? super MongodConnection> getConnectionCommandsExecutor();

  RequiredTransaction classify(Command<?, ?> command);

  Stream<Command<?, ?>> streamAllCommands();

}
