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

package com.torodb.mongodb.commands.signatures.authentication;

import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandImplementation;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Iterator;
import java.util.Map;

/**
 *
 */
public class AuthenticationCommands implements Iterable<Command> {

  private final ImmutableList<Command> commands = ImmutableList.<Command>of(
      GetNonceCommand.INSTANCE
  );

  @Override
  public Iterator<Command> iterator() {
    return commands.iterator();
  }

  @SuppressWarnings("checkstyle:LineLength")
  public abstract static class AuthenticationCommandsImplementationsBuilder<ContextT> implements
      Iterable<Map.Entry<Command<?, ?>, CommandImplementation<?, ?, ? super ContextT>>> {

    public abstract CommandImplementation<Empty, String, ? super ContextT> getGetNonceImplementation();

    private Map<Command<?, ?>, CommandImplementation<?, ?, ? super ContextT>> createMap() {
      return ImmutableMap.<Command<?, ?>, CommandImplementation<?, ?, ? super ContextT>>builder()
          .put(GetNonceCommand.INSTANCE, getGetNonceImplementation())
          .build();
    }

    @Override
    public Iterator<Map.Entry<Command<?, ?>, CommandImplementation<?, ?, ? super ContextT>>> iterator() {
      return createMap().entrySet().iterator();
    }

  }
}
