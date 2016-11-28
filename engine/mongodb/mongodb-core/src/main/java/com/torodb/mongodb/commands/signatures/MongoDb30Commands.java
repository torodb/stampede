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

package com.torodb.mongodb.commands.signatures;

import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandImplementation;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.torodb.mongodb.commands.signatures.admin.AdminCommands;
import com.torodb.mongodb.commands.signatures.admin.AdminCommands.AdminCommandsImplementationsBuilder;
import com.torodb.mongodb.commands.signatures.aggregation.AggregationCommands;
import com.torodb.mongodb.commands.signatures.aggregation.AggregationCommands.AggregationCommandsImplementationsBuilder;
import com.torodb.mongodb.commands.signatures.authentication.AuthenticationCommands;
import com.torodb.mongodb.commands.signatures.authentication.AuthenticationCommands.AuthenticationCommandsImplementationsBuilder;
import com.torodb.mongodb.commands.signatures.diagnostic.DiagnosticCommands;
import com.torodb.mongodb.commands.signatures.diagnostic.DiagnosticCommands.DiagnosticCommandsImplementationsBuilder;
import com.torodb.mongodb.commands.signatures.general.GeneralCommands;
import com.torodb.mongodb.commands.signatures.general.GeneralCommands.GeneralCommandsImplementationsBuilder;
import com.torodb.mongodb.commands.signatures.internal.InternalCommands;
import com.torodb.mongodb.commands.signatures.internal.InternalCommands.InternalCommandsImplementationsBuilder;
import com.torodb.mongodb.commands.signatures.repl.ReplCommands;
import com.torodb.mongodb.commands.signatures.repl.ReplCommands.ReplCommandsImplementationsBuilder;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 *
 */
public class MongoDb30Commands implements Iterable<Command> {

  private final ImmutableList<Command> commands;

  @SuppressWarnings("unchecked")
  public MongoDb30Commands() {
    commands = ImmutableList.copyOf(
        Iterables.concat(
            new AdminCommands(),
            new AggregationCommands(),
            new AuthenticationCommands(),
            new DiagnosticCommands(),
            new GeneralCommands(),
            new InternalCommands(),
            new ReplCommands()
        )
    );
  }

  @Override
  public Iterator<Command> iterator() {
    return commands.iterator();
  }

  @SuppressWarnings("checkstyle:LineLength")
  public static class MongoDb30CommandsImplementationBuilder<ContextT> implements
      Iterable<Map.Entry<Command<?, ?>, CommandImplementation<?, ?, ? super ContextT>>> {

    private final AdminCommandsImplementationsBuilder<ContextT> adminCommandsImplementationsBuilder;
    private final AggregationCommandsImplementationsBuilder<ContextT> aggregationImplementationsBuilder;
    private final AuthenticationCommandsImplementationsBuilder<ContextT> authenticationCommandsImplementationsBuilder;
    private final DiagnosticCommandsImplementationsBuilder<ContextT> diagnosticImplementationsBuilder;
    private final GeneralCommandsImplementationsBuilder<ContextT> generalImplementationsBuilder;
    private final InternalCommandsImplementationsBuilder<ContextT> internalCommandsImplementationsBuilder;
    private final ReplCommandsImplementationsBuilder<ContextT> replCommandsImplementationsBuilder;

    public MongoDb30CommandsImplementationBuilder(
        AdminCommandsImplementationsBuilder<ContextT> adminCommandsImplementationsBuilder,
        AggregationCommandsImplementationsBuilder<ContextT> aggregationImplementationsBuilder,
        AuthenticationCommandsImplementationsBuilder<ContextT> authenticationCommandsImplementationsBuilder,
        DiagnosticCommandsImplementationsBuilder<ContextT> diagnosticImplementationsBuilder,
        GeneralCommandsImplementationsBuilder<ContextT> generalImplementationsBuilder,
        InternalCommandsImplementationsBuilder<ContextT> internalCommandsImplementationsBuilder,
        ReplCommandsImplementationsBuilder<ContextT> replCommandsImplementationsBuilder) {
      this.adminCommandsImplementationsBuilder =
          adminCommandsImplementationsBuilder;
      this.aggregationImplementationsBuilder =
          aggregationImplementationsBuilder;
      this.authenticationCommandsImplementationsBuilder =
          authenticationCommandsImplementationsBuilder;
      this.diagnosticImplementationsBuilder =
          diagnosticImplementationsBuilder;
      this.generalImplementationsBuilder = generalImplementationsBuilder;
      this.internalCommandsImplementationsBuilder =
          internalCommandsImplementationsBuilder;
      this.replCommandsImplementationsBuilder =
          replCommandsImplementationsBuilder;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterator<Entry<Command<?, ?>, CommandImplementation<?, ?, ? super ContextT>>> iterator() {
      return Iterators.concat(
          adminCommandsImplementationsBuilder.iterator(),
          aggregationImplementationsBuilder.iterator(),
          authenticationCommandsImplementationsBuilder.iterator(),
          diagnosticImplementationsBuilder.iterator(),
          generalImplementationsBuilder.iterator(),
          internalCommandsImplementationsBuilder.iterator(),
          replCommandsImplementationsBuilder.iterator()
      );
    }

  }
}
