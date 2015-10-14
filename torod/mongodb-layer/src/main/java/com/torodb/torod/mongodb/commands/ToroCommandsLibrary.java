
package com.torodb.torod.mongodb.commands;

import com.eightkdata.mongowp.mongoserver.api.safe.Command;
import com.eightkdata.mongowp.mongoserver.api.safe.CommandImplementation;
import com.eightkdata.mongowp.mongoserver.api.safe.CommandsLibrary;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.GroupedCommandsLibrary;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.NameBasedCommandsLibrary;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.MongoDb30Commands;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.torodb.torod.core.BuildProperties;
import com.torodb.torod.mongodb.unsafe.UnsafeCommandsLibraryAdaptor;
import java.util.Collection;
import java.util.Map.Entry;
import javax.inject.Inject;

/**
 *
 */
public class ToroCommandsLibrary extends GroupedCommandsLibrary {
    private static final CommandsLibrary SAFE_LIBRARY = new NameBasedCommandsLibrary("mongo-3.0-partial", new MongoDb30Commands());

    @Inject
    public ToroCommandsLibrary(
            BuildProperties buildProperties,
            ToroSafeCommandTool toroSafeCommandTool) {
        super(
            "toro-" + buildProperties.getFullVersion(),
            getSubLibraries(buildProperties.getFullVersion(), toroSafeCommandTool)
        );
    }

    public static CommandsLibrary getSafeLibrary() {
        return SAFE_LIBRARY;
    }

    public static ImmutableList<CommandsLibrary> getSubLibraries(
            String toroVersion,
            ToroSafeCommandTool toroSafeCommandTool) {


        return ImmutableList.<CommandsLibrary>builder()
//                .add(SAFE_LIBRARY) //TODO: use this once we implemented all unsafe commands
                .add(getImplementedSafeLibrary(toroVersion, toroSafeCommandTool))
                .add(new UnsafeCommandsLibraryAdaptor("toro-" + toroVersion + "-standard-unsafe"))
                .build();
    }

    public static CommandsLibrary getImplementedSafeLibrary(
            String toroVersion,
            ToroSafeCommandTool toroSafeCommandTool) {
        ImmutableMap<Command, CommandImplementation> safeCommandImplementations = toroSafeCommandTool.getMap();
        Collection<Command> implementedSafeCommands = Lists.newArrayList();
        for (Entry<Command, CommandImplementation> entry : safeCommandImplementations.entrySet()) {
            if (entry.getValue() instanceof NotImplementedCommandImplementation) {
                continue;
            }
            implementedSafeCommands.add(entry.getKey());
        }
        return new NameBasedCommandsLibrary(toroVersion, implementedSafeCommands);
    }

}
