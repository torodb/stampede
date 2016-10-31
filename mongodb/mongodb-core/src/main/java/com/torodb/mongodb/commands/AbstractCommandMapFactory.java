package com.torodb.mongodb.commands;

import java.util.Map.Entry;
import java.util.function.Supplier;

import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.MongoDb30Commands.MongoDb30CommandsImplementationBuilder;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.AdminCommands.AdminCommandsImplementationsBuilder;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.aggregation.AggregationCommands.AggregationCommandsImplementationsBuilder;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.authentication.AuthenticationCommands.AuthenticationCommandsImplementationsBuilder;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.DiagnosticCommands.DiagnosticCommandsImplementationsBuilder;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.GeneralCommands.GeneralCommandsImplementationsBuilder;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.internal.InternalCommands.InternalCommandsImplementationsBuilder;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.repl.ReplCommands.ReplCommandsImplementationsBuilder;
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
