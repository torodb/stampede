/*
 * This file is part of ToroDB.
 *
 * ToroDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ToroDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with repl. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.mongodb.repl.commands;

import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.*;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.repl.ApplyOpsCommand;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandsLibrary;
import com.eightkdata.mongowp.server.api.impl.NameBasedCommandsLibrary;
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
                .addAsAlias(LogAndIgnoreCommand.INSTANCE, CollModCommand.INSTANCE.getCommandName())
                .addAsAlias(LogAndIgnoreCommand.INSTANCE, "coverToCapped")
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
