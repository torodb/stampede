
package com.torodb.torod.mongodb.commands;

import com.eightkdata.mongowp.server.api.CommandsExecutor;
import com.eightkdata.mongowp.server.api.CommandsLibrary;
import com.eightkdata.mongowp.server.api.deprecated.QueryCommandProcessor;
import com.eightkdata.mongowp.server.api.impl.GroupedCommandsExecutor;
import com.eightkdata.mongowp.server.api.impl.MapBasedCommandsExecutor;
import com.google.common.collect.ImmutableList;
import com.torodb.torod.mongodb.unsafe.UnsafeCommandsExecutorAdaptor;
import javax.inject.Inject;
import javax.inject.Provider;

/**
 *
 */
public class ToroCommandsExecutorProvider implements Provider<CommandsExecutor> {

    private final CommandsLibrary library;
    private final ToroV30CommandTool toroSafeCommandTool;
    private final QueryCommandProcessor unsafeCommandProcessor;
    private final ToroDBSpecificCommandTool toroDBSpecificCommandTool;

    @Inject
    public ToroCommandsExecutorProvider(
            CommandsLibrary library,
            ToroV30CommandTool toroSafeCommandTool,
            QueryCommandProcessor unsafeCommandProcessor,
            ToroDBSpecificCommandTool toroDBSpecificCommandTool) {
        this.library = library;
        this.toroSafeCommandTool = toroSafeCommandTool;
        this.unsafeCommandProcessor = unsafeCommandProcessor;
        this.toroDBSpecificCommandTool = toroDBSpecificCommandTool;
    }

    @Override
    public CommandsExecutor get() {
        return new GroupedCommandsExecutor(
                ImmutableList.<CommandsExecutor>builder()
                .add(createSafeCommandsExecutor(toroSafeCommandTool, toroDBSpecificCommandTool))
                .add(new UnsafeCommandsExecutorAdaptor(unsafeCommandProcessor))
                .build()
        );
    }

    private CommandsExecutor createSafeCommandsExecutor(
            ToroV30CommandTool toroSafeCommandTool,
            ToroDBSpecificCommandTool toroDBSpecificCommandTool) {
        return MapBasedCommandsExecutor.fromLibraryBuilder(library)
                .addImplementations(toroSafeCommandTool.getMap().entrySet())
                .addImplementations(toroDBSpecificCommandTool.getMap().entrySet())
                .build();
    }


}
