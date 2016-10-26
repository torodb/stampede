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

import com.google.inject.PrivateModule;
import com.google.inject.Singleton;

/**
 *
 */
public class ReplCommandsGuiceModule extends PrivateModule {

    @Override
    protected void configure() {
        bind(ReplCommandsLibrary.class)
                .in(Singleton.class);
        expose(ReplCommandsLibrary.class);

        bind(ReplCommandsExecutor.class)
                .in(Singleton.class);
        expose(ReplCommandsExecutor.class);

        bindImplementations();
    }

    private void bindImplementations() {
        bind(CreateCollectionReplImpl.class);
        bind(CreateIndexesReplImpl.class);
        bind(DropCollectionReplImpl.class);
        bind(DropDatabaseReplImpl.class);
        bind(LogAndIgnoreReplImpl.class);
        bind(LogAndStopReplImpl.class);
    }

}
