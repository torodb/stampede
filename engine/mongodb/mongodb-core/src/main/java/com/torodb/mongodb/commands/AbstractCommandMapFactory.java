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
import com.eightkdata.mongowp.server.api.CommandImplementation;
import com.google.common.collect.ImmutableMap;
import com.torodb.mongodb.commands.impl.NotImplementedCommandImplementation;
import com.torodb.mongodb.commands.signatures.MongoDb30Commands.MongoDb30CommandsImplementationBuilder;
import com.torodb.mongodb.commands.signatures.admin.AdminCommands.AdminCommandsImplementationsBuilder;
import com.torodb.mongodb.commands.signatures.aggregation.AggregationCommands.AggregationCommandsImplementationsBuilder;
import com.torodb.mongodb.commands.signatures.authentication.AuthenticationCommands.AuthenticationCommandsImplementationsBuilder;
import com.torodb.mongodb.commands.signatures.diagnostic.DiagnosticCommands.DiagnosticCommandsImplementationsBuilder;
import com.torodb.mongodb.commands.signatures.general.GeneralCommands.GeneralCommandsImplementationsBuilder;
import com.torodb.mongodb.commands.signatures.internal.InternalCommands.InternalCommandsImplementationsBuilder;
import com.torodb.mongodb.commands.signatures.repl.ReplCommands.ReplCommandsImplementationsBuilder;

import java.util.Map.Entry;
import java.util.function.Supplier;

public class AbstractCommandMapFactory<ContextT> implements
    Supplier<ImmutableMap<Command<?, ?>, CommandImplementation<?, ?, ? super ContextT>>> {

  private final AdminCommandsImplementationsBuilder<ContextT> adminBuilder;
  private final AggregationCommandsImplementationsBuilder<ContextT> aggregationBuilder;
  @SuppressWarnings("checkstyle:LineLength")
  private final AuthenticationCommandsImplementationsBuilder<ContextT> authenticationCommandsImplementationsBuilder;
  private final DiagnosticCommandsImplementationsBuilder<ContextT> diagnosticBuilder;
  private final GeneralCommandsImplementationsBuilder<ContextT> generalBuilder;
  private final InternalCommandsImplementationsBuilder<ContextT> internalBuilder;
  private final ReplCommandsImplementationsBuilder<ContextT> replBuilder;

  @SuppressWarnings("checkstyle:LineLength")
  public AbstractCommandMapFactory(AdminCommandsImplementationsBuilder<ContextT> adminBuilder,
      AggregationCommandsImplementationsBuilder<ContextT> aggregationBuilder,
      AuthenticationCommandsImplementationsBuilder<ContextT> authenticationCommandsImplementationsBuilder,
      DiagnosticCommandsImplementationsBuilder<ContextT> diagnosticBuilder,
      GeneralCommandsImplementationsBuilder<ContextT> generalBuilder,
      InternalCommandsImplementationsBuilder<ContextT> internalBuilder,
      ReplCommandsImplementationsBuilder<ContextT> replBuilder) {
    super();
    this.adminBuilder = adminBuilder;
    this.aggregationBuilder = aggregationBuilder;
    this.authenticationCommandsImplementationsBuilder = authenticationCommandsImplementationsBuilder;
    this.diagnosticBuilder = diagnosticBuilder;
    this.generalBuilder = generalBuilder;
    this.internalBuilder = internalBuilder;
    this.replBuilder = replBuilder;
  }

  @Override
  public ImmutableMap<Command<?, ?>, CommandImplementation<?, ?, ? super ContextT>> get() {
    MongoDb30CommandsImplementationBuilder<ContextT> implBuilder =
        new MongoDb30CommandsImplementationBuilder<>(
            adminBuilder, aggregationBuilder, authenticationCommandsImplementationsBuilder,
            diagnosticBuilder, generalBuilder, internalBuilder, replBuilder
        );

    ImmutableMap.Builder<Command<?, ?>, CommandImplementation<?, ?, ? super ContextT>> builder =
        ImmutableMap.builder();
    for (Entry<Command<?, ?>, CommandImplementation<?, ?, ? super ContextT>> entry : implBuilder) {
      if (entry.getValue() instanceof NotImplementedCommandImplementation) {
        continue;
      }
      builder.put(entry.getKey(), entry.getValue());
    }

    return builder.build();
  }
}
