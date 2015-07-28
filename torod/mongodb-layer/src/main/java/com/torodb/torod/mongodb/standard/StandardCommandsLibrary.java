
package com.torodb.torod.mongodb.standard;

import com.eightkdata.mongowp.mongoserver.api.safe.CommandsLibrary;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.GroupedCommandsLibrary;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.NameBasedCommandsLibrary;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.MongoDb30Commands;
import com.google.common.collect.ImmutableList;
import com.torodb.torod.core.BuildProperties;
import com.torodb.torod.mongodb.unsafe.UnsafeCommandsLibraryAdaptor;
import javax.inject.Inject;

/**
 *
 */
public class StandardCommandsLibrary extends GroupedCommandsLibrary {

    private static final CommandsLibrary SAFE_LIBRARY = new NameBasedCommandsLibrary("mongo-3.0-partial", new MongoDb30Commands());

    @Inject
    public StandardCommandsLibrary(BuildProperties buildProperties) {
        super(
            "toro-" + buildProperties.getFullVersion() + "-standard",
            getSubLibraries(buildProperties.getFullVersion())
        );
    }

    public static CommandsLibrary getSafeLibrary() {
        return SAFE_LIBRARY;
    }

    public static ImmutableList<CommandsLibrary> getSubLibraries(String toroVersion) {
        return ImmutableList.<CommandsLibrary>builder()
                .add(SAFE_LIBRARY)
                .add(new UnsafeCommandsLibraryAdaptor("toro-" + toroVersion + "-standard-unsafe"))
                .build();
    }

}
