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

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.*;
import com.eightkdata.mongowp.server.api.*;
import com.eightkdata.mongowp.server.api.impl.MapBasedCommandsExecutor;
import com.torodb.core.supervision.Supervisor;
import com.torodb.torod.SharedWriteTorodTransaction;

/**
 *
 */
public final class ReplCommandsExecutor implements CommandsExecutor<SharedWriteTorodTransaction> {

    private final MapBasedCommandsExecutor<SharedWriteTorodTransaction> delegate;

    public ReplCommandsExecutor(ReplCommandsLibrary library, Supervisor supervisor) {
        delegate = MapBasedCommandsExecutor
                .<SharedWriteTorodTransaction>fromLibraryBuilder(library)
                .addImplementation(LogAndStopCommand.INSTANCE, new LogAndStopReplImpl(supervisor))
                .addImplementation(LogAndIgnoreCommand.INSTANCE, new LogAndIgnoreReplImpl())
//                .addImplementation(ApplyOpsCommand.INSTANCE, whatever)
//                .addImplementation(colmod, whatever)
//                .addImplementation(coverToCapped, whatever)
                .addImplementation(CreateCollectionCommand.INSTANCE, new CreateCollectionReplImpl())
                .addImplementation(DropCollectionCommand.INSTANCE, new DropCollectionReplImpl())
                .addImplementation(DropDatabaseCommand.INSTANCE, new DropDatabaseReplImpl())
                .addImplementation(DropIndexesCommand.INSTANCE, new DropIndexesReplImpl())
//                .addImplementation(emptycapped, whatever)

                .addImplementation(CreateIndexesCommand.INSTANCE, new CreateIndexesReplImpl())
                .build();
    }

    @Override
    public <Arg, Result> Status<Result> execute(Request request,
            Command<? super Arg, ? super Result> command, Arg arg,
            SharedWriteTorodTransaction context) {
        return delegate.execute(request, command, arg, context);
    }
}
