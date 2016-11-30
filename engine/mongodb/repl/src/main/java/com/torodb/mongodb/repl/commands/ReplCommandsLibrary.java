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

package com.torodb.mongodb.repl.commands;

import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandsLibrary;
import com.eightkdata.mongowp.server.api.impl.NameBasedCommandsLibrary;
import com.torodb.mongodb.commands.signatures.admin.CollModCommand;
import com.torodb.mongodb.commands.signatures.admin.CreateCollectionCommand;
import com.torodb.mongodb.commands.signatures.admin.CreateIndexesCommand;
import com.torodb.mongodb.commands.signatures.admin.DropCollectionCommand;
import com.torodb.mongodb.commands.signatures.admin.DropDatabaseCommand;
import com.torodb.mongodb.commands.signatures.admin.DropIndexesCommand;
import com.torodb.mongodb.commands.signatures.admin.RenameCollectionCommand;
import com.torodb.mongodb.commands.signatures.repl.ApplyOpsCommand;

import java.util.Set;

/**
 *
 */
public class ReplCommandsLibrary implements CommandsLibrary {

  private final NameBasedCommandsLibrary delegate;

  public ReplCommandsLibrary() {

    Command<?, ?> dropIndexes =
        DropIndexesCommand.INSTANCE;

    delegate = new NameBasedCommandsLibrary.Builder("repl-3.2")
        .addAsAlias(LogAndStopCommand.INSTANCE, ApplyOpsCommand.INSTANCE.getCommandName())
        .addCommand(CreateCollectionCommand.INSTANCE)
        .addCommand(DropCollectionCommand.INSTANCE)
        .addCommand(DropDatabaseCommand.INSTANCE)
        .addCommand(RenameCollectionCommand.INSTANCE)
        .addAsAlias(LogAndIgnoreCommand.INSTANCE, CollModCommand.INSTANCE.getCommandName())
        .addAsAlias(LogAndIgnoreCommand.INSTANCE, "convertToCapped")
        .addAsAlias(LogAndIgnoreCommand.INSTANCE, "emptycapped")
        //drop indexes aliases
        .addAsAlias(dropIndexes, "deleteIndex")
        .addAsAlias(dropIndexes, "deleteIndexes")
        .addAsAlias(dropIndexes, "dropIndex")
        .addAsAlias(dropIndexes, "dropIndexes")
        //called as an insert
        .addCommand(CreateIndexesCommand.INSTANCE)
        .build();
  }

  @Override
  public String getSupportedVersion() {
    return "repl-3.2";
  }

  @Override
  public Set<Command> getSupportedCommands() {
    return delegate.getSupportedCommands();
  }

  @Override
  public LibraryEntry find(BsonDocument requestDocument) {
    return delegate.find(requestDocument);
  }
}
