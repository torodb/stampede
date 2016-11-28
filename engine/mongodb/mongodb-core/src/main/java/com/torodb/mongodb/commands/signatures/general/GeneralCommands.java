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

package com.torodb.mongodb.commands.signatures.general;

import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandImplementation;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.torodb.mongodb.commands.signatures.general.DeleteCommand.DeleteArgument;
import com.torodb.mongodb.commands.signatures.general.FindCommand.FindArgument;
import com.torodb.mongodb.commands.signatures.general.FindCommand.FindResult;
import com.torodb.mongodb.commands.signatures.general.GetLastErrorCommand.GetLastErrorArgument;
import com.torodb.mongodb.commands.signatures.general.GetLastErrorCommand.GetLastErrorReply;
import com.torodb.mongodb.commands.signatures.general.InsertCommand.InsertArgument;
import com.torodb.mongodb.commands.signatures.general.InsertCommand.InsertResult;
import com.torodb.mongodb.commands.signatures.general.UpdateCommand.UpdateArgument;
import com.torodb.mongodb.commands.signatures.general.UpdateCommand.UpdateResult;

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

  @SuppressWarnings("checkstyle:LineLength")
  public abstract static class GeneralCommandsImplementationsBuilder<ContextT> implements
      Iterable<Map.Entry<Command<?, ?>, CommandImplementation<?, ?, ? super ContextT>>> {

    public abstract CommandImplementation<GetLastErrorArgument, GetLastErrorReply, ? super ContextT> getGetLastErrrorImplementation();

    public abstract CommandImplementation<InsertArgument, InsertResult, ? super ContextT> getInsertImplementation();

    public abstract CommandImplementation<DeleteArgument, Long, ? super ContextT> getDeleteImplementation();

    public abstract CommandImplementation<FindArgument, FindResult, ? super ContextT> getFindImplementation();

    public abstract CommandImplementation<UpdateArgument, UpdateResult, ? super ContextT> getUpdateImplementation();

    private Map<Command<?, ?>, CommandImplementation<?, ?, ? super ContextT>> createMap() {
      return ImmutableMap.<Command<?, ?>, CommandImplementation<?, ?, ? super ContextT>>builder()
          .put(DeleteCommand.INSTANCE, getDeleteImplementation())
          .put(FindCommand.INSTANCE, getFindImplementation())
          .put(InsertCommand.INSTANCE, getInsertImplementation())
          .put(GetLastErrorCommand.INSTANCE, getGetLastErrrorImplementation())
          .put(UpdateCommand.INSTANCE, getUpdateImplementation())
          .build();
    }

    @Override
    public Iterator<Entry<Command<?, ?>, CommandImplementation<?, ?, ? super ContextT>>> iterator() {
      return createMap().entrySet().iterator();
    }

  }

}
