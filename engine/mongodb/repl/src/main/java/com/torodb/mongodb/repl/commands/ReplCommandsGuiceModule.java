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

import com.google.inject.PrivateModule;
import com.google.inject.Singleton;
import com.torodb.mongodb.repl.commands.impl.CreateCollectionReplImpl;
import com.torodb.mongodb.repl.commands.impl.CreateIndexesReplImpl;
import com.torodb.mongodb.repl.commands.impl.DropCollectionReplImpl;
import com.torodb.mongodb.repl.commands.impl.DropDatabaseReplImpl;
import com.torodb.mongodb.repl.commands.impl.DropIndexesReplImpl;
import com.torodb.mongodb.repl.commands.impl.LogAndIgnoreReplImpl;
import com.torodb.mongodb.repl.commands.impl.LogAndStopReplImpl;
import com.torodb.mongodb.repl.commands.impl.RenameCollectionReplImpl;

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
    bind(DropIndexesReplImpl.class);
    bind(LogAndIgnoreReplImpl.class);
    bind(LogAndStopReplImpl.class);
    bind(RenameCollectionReplImpl.class);
  }

}
