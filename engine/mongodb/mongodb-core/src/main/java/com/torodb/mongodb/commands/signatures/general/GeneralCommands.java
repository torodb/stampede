/*
 * ToroDB - ToroDB: MongoDB Core
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.torodb.mongodb.commands.signatures.general;

import com.torodb.mongodb.commands.signatures.general.DeleteCommand.DeleteArgument;
import com.torodb.mongodb.commands.signatures.general.FindCommand.FindArgument;
import com.torodb.mongodb.commands.signatures.general.FindCommand.FindResult;
import com.torodb.mongodb.commands.signatures.general.GetLastErrorCommand.GetLastErrorArgument;
import com.torodb.mongodb.commands.signatures.general.GetLastErrorCommand.GetLastErrorReply;
import com.torodb.mongodb.commands.signatures.general.InsertCommand.InsertArgument;
import com.torodb.mongodb.commands.signatures.general.InsertCommand.InsertResult;
import com.torodb.mongodb.commands.signatures.general.UpdateCommand.UpdateArgument;
import com.torodb.mongodb.commands.signatures.general.UpdateCommand.UpdateResult;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandImplementation;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 *
 */
public class GeneralCommands implements Iterable<Command> {

    private final ImmutableList<Command> commands = ImmutableList.<Command>of(
            DeleteCommand.INSTANCE,
            InsertCommand.INSTANCE,
            GetLastErrorCommand.INSTANCE,
            UpdateCommand.INSTANCE
    );

    @Override
    public Iterator<Command> iterator() {
        return commands.iterator();
    }

    public static abstract class GeneralCommandsImplementationsBuilder<Context> implements Iterable<Map.Entry<Command<?,?>, CommandImplementation<?, ?, ? super Context>>> {

        public abstract CommandImplementation<GetLastErrorArgument, GetLastErrorReply, ? super Context> getGetLastErrrorImplementation();

        public abstract CommandImplementation<InsertArgument, InsertResult, ? super Context> getInsertImplementation();

        public abstract CommandImplementation<DeleteArgument, Long, ? super Context> getDeleteImplementation();

        public abstract CommandImplementation<FindArgument, FindResult, ? super Context> getFindImplementation();

        public abstract CommandImplementation<UpdateArgument, UpdateResult, ? super Context> getUpdateImplementation();

        private Map<Command<?,?>, CommandImplementation<?, ?, ? super Context>> createMap() {
            return ImmutableMap.<Command<?,?>, CommandImplementation<?, ?, ? super Context>>builder()
                    .put(DeleteCommand.INSTANCE, getDeleteImplementation())
                    .put(FindCommand.INSTANCE, getFindImplementation())
                    .put(InsertCommand.INSTANCE, getInsertImplementation())
                    .put(GetLastErrorCommand.INSTANCE, getGetLastErrrorImplementation())
                    .put(UpdateCommand.INSTANCE, getUpdateImplementation())
                    .build();
        }

        @Override
        public Iterator<Entry<Command<?,?>, CommandImplementation<?, ?, ? super Context>>> iterator() {
            return createMap().entrySet().iterator();
        }

    }
    
}
