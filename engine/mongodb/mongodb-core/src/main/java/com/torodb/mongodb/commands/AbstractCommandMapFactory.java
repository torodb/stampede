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
package com.torodb.mongodb.commands;

import java.util.Map.Entry;
import java.util.function.Supplier;

import com.torodb.mongodb.commands.signatures.MongoDb30Commands.MongoDb30CommandsImplementationBuilder;
import com.torodb.mongodb.commands.signatures.admin.AdminCommands.AdminCommandsImplementationsBuilder;
import com.torodb.mongodb.commands.signatures.aggregation.AggregationCommands.AggregationCommandsImplementationsBuilder;
import com.torodb.mongodb.commands.signatures.authentication.AuthenticationCommands.AuthenticationCommandsImplementationsBuilder;
import com.torodb.mongodb.commands.signatures.diagnostic.DiagnosticCommands.DiagnosticCommandsImplementationsBuilder;
import com.torodb.mongodb.commands.signatures.general.GeneralCommands.GeneralCommandsImplementationsBuilder;
import com.torodb.mongodb.commands.signatures.internal.InternalCommands.InternalCommandsImplementationsBuilder;
import com.torodb.mongodb.commands.signatures.repl.ReplCommands.ReplCommandsImplementationsBuilder;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandImplementation;
import com.google.common.collect.ImmutableMap;
import com.torodb.mongodb.commands.impl.NotImplementedCommandImplementation;

public class AbstractCommandMapFactory<Context> implements Supplier<ImmutableMap<Command<?,?>, CommandImplementation<?, ?, ? super Context>>> {
    private final AdminCommandsImplementationsBuilder<Context> adminBuilder;
    private final AggregationCommandsImplementationsBuilder<Context> aggregationBuilder;
    private final AuthenticationCommandsImplementationsBuilder<Context> authenticationCommandsImplementationsBuilder;
    private final DiagnosticCommandsImplementationsBuilder<Context> diagnosticBuilder;
    private final GeneralCommandsImplementationsBuilder<Context> generalBuilder;
    private final InternalCommandsImplementationsBuilder<Context> internalBuilder;
    private final ReplCommandsImplementationsBuilder<Context> replBuilder;
    
    public AbstractCommandMapFactory(AdminCommandsImplementationsBuilder<Context> adminBuilder,
            AggregationCommandsImplementationsBuilder<Context> aggregationBuilder,
            AuthenticationCommandsImplementationsBuilder<Context> authenticationCommandsImplementationsBuilder,
            DiagnosticCommandsImplementationsBuilder<Context> diagnosticBuilder,
            GeneralCommandsImplementationsBuilder<Context> generalBuilder,
            InternalCommandsImplementationsBuilder<Context> internalBuilder,
            ReplCommandsImplementationsBuilder<Context> replBuilder) {
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
    public ImmutableMap<Command<?,?>, CommandImplementation<?, ?, ? super Context>> get() {
        MongoDb30CommandsImplementationBuilder<Context> implBuilder = new MongoDb30CommandsImplementationBuilder<>(
                adminBuilder, aggregationBuilder, authenticationCommandsImplementationsBuilder, diagnosticBuilder, generalBuilder, internalBuilder, replBuilder
        );

        ImmutableMap.Builder<Command<?,?>, CommandImplementation<?, ?, ? super Context>> builder = ImmutableMap.builder();
        for (Entry<Command<?,?>, CommandImplementation<?, ?, ? super Context>> entry : implBuilder) {
            if (entry.getValue() instanceof NotImplementedCommandImplementation) {
                continue;
            }
            builder.put(entry.getKey(), entry.getValue());
        }

        return builder.build();
    }
}
