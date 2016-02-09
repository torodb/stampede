
package com.torodb.torod.mongodb.commands;

import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandImplementation;
import com.eightkdata.mongowp.server.api.CommandsLibrary;
import com.eightkdata.mongowp.server.api.impl.GroupedCommandsLibrary;
import com.eightkdata.mongowp.server.api.impl.NameBasedCommandsLibrary;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.torodb.torod.core.BuildProperties;
import com.torodb.torod.mongodb.unsafe.UnsafeCommandsLibraryAdaptor;
import java.util.Collection;
import java.util.Map.Entry;
import javax.inject.Inject;
import javax.inject.Provider;

/**
 *
 */
public final class ToroCommandsLibraryProvider implements Provider<CommandsLibrary> {
    private final String toroVersion;
    private final ToroV30CommandTool toroSafeCommandTool;
    private final ToroDBSpecificCommandTool toroDBSpecificCommandTool;

    @Inject
    public ToroCommandsLibraryProvider(
            BuildProperties buildProperties,
            ToroV30CommandTool toroSafeCommandTool,
            ToroDBSpecificCommandTool toroDBSpecificCommandTool) {
        this.toroVersion = buildProperties.getFullVersion();
        this.toroSafeCommandTool = toroSafeCommandTool;
        this.toroDBSpecificCommandTool = toroDBSpecificCommandTool;
    }

    @Override
    public CommandsLibrary get() {
        String supportedVersion = "toro-" + toroVersion;

        return new GroupedCommandsLibrary(
                supportedVersion,
                getSubLibraries(toroSafeCommandTool, toroDBSpecificCommandTool)
        );
    }

    private ImmutableList<CommandsLibrary> getSubLibraries(
            ToroV30CommandTool toroSafeCommandTool,
            ToroDBSpecificCommandTool toroDBSpecificCommandTool) {


        return ImmutableList.<CommandsLibrary>builder()
                .add(getImplementedSafeLibrary(toroVersion, toroSafeCommandTool, toroDBSpecificCommandTool))
                .add(new UnsafeCommandsLibraryAdaptor("toro-" + toroVersion + "-standard-unsafe"))
                .build();
    }

    private CommandsLibrary getImplementedSafeLibrary(
            String toroVersion,
            ToroV30CommandTool toroSafeCommandTool,
            ToroDBSpecificCommandTool toroDBSpecificCommandTool) {
        ImmutableMap<Command<?,?>, CommandImplementation> safeCommandImplementations = toroSafeCommandTool.getMap();
        Collection<Command> implementedSafeCommands = Lists.newArrayList();
        for (Entry<Command<?,?>, CommandImplementation> entry : safeCommandImplementations.entrySet()) {
            if (entry.getValue() instanceof NotImplementedCommandImplementation) {
                continue;
            }
            implementedSafeCommands.add(entry.getKey());
        }
        
        for (Entry<Command<?,?>, CommandImplementation> entry : toroDBSpecificCommandTool.getMap().entrySet()) {
            implementedSafeCommands.add(entry.getKey());
        }

        return new NameBasedCommandsLibrary(toroVersion, implementedSafeCommands);
    }

}
