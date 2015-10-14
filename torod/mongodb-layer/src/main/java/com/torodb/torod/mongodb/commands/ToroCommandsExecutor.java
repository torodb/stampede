
package com.torodb.torod.mongodb.commands;

import com.eightkdata.mongowp.mongoserver.api.QueryCommandProcessor;
import com.eightkdata.mongowp.mongoserver.api.safe.CommandsExecutor;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.GroupedCommandsExecutor;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.MapBasedCommandsExecutor;
import com.google.common.collect.ImmutableList;
import com.torodb.torod.mongodb.unsafe.UnsafeCommandsExecutorAdaptor;
import javax.inject.Inject;

/**
 *
 */
public class ToroCommandsExecutor extends GroupedCommandsExecutor {

    @Inject
    public ToroCommandsExecutor(
            ToroSafeCommandTool toroSafeCommandTool,
            QueryCommandProcessor unsafeCommandProcessor) {
        super(getSubExecutors(toroSafeCommandTool, unsafeCommandProcessor));
    }

    public static ImmutableList<CommandsExecutor> getSubExecutors(
            ToroSafeCommandTool toroSafeCommandTool,
            QueryCommandProcessor unsafeCommandProcessor) {
        return ImmutableList.<CommandsExecutor>builder()
                .add(createSafeCommandsExecutor(toroSafeCommandTool))
                .add(new UnsafeCommandsExecutorAdaptor(unsafeCommandProcessor))
                .build();
    }

    public static CommandsExecutor createSafeCommandsExecutor(
            ToroSafeCommandTool toroSafeCommandTool) {
        return MapBasedCommandsExecutor.fromLibraryBuilder(ToroCommandsLibrary.getSafeLibrary())
                .addImplementations(toroSafeCommandTool.getMap().entrySet())
                .build();
    }


}
