
package com.torodb.torod.mongodb.unsafe;

import com.eightkdata.mongowp.mongoserver.api.QueryCommandProcessor.QueryCommand;
import com.eightkdata.mongowp.mongoserver.api.QueryCommandProcessor.QueryCommandGroup;
import com.eightkdata.mongowp.mongoserver.api.safe.Command;
import com.eightkdata.mongowp.mongoserver.api.safe.CommandsLibrary;
import java.util.Set;
import org.bson.BsonDocument;

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
