
package com.torodb.torod.mongodb.unsafe;

import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandsLibrary;
import com.eightkdata.mongowp.server.api.deprecated.QueryCommandProcessor.QueryCommand;
import com.eightkdata.mongowp.server.api.deprecated.QueryCommandProcessor.QueryCommandGroup;
import java.util.Set;

/**
 *
 */
public class UnsafeCommandsLibraryAdaptor implements CommandsLibrary {

    private final String supportedVersion;

    public UnsafeCommandsLibraryAdaptor(String supportedVersion) {
        this.supportedVersion = supportedVersion;
    }

    @Override
    public String getSupportedVersion() {
        return supportedVersion;
    }

    @Override
    public Set<Command> getSupportedCommands() {
        return null;
    }

    @Override
    public Command find(BsonDocument requestDocument) {

        QueryCommand queryCommand = QueryCommandGroup.byQueryDocument(requestDocument);

        if (null == queryCommand) {
            return null;
        }

        return new UnsafeCommand(queryCommand);
    }

}
