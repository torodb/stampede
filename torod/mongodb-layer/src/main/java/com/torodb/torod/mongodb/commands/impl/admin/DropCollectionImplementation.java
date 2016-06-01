
package com.torodb.torod.mongodb.commands.impl.admin;

import com.eightkdata.mongowp.exceptions.BadValueException;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandRequest;
import com.eightkdata.mongowp.server.api.CommandResult;
import com.eightkdata.mongowp.server.api.impl.CollectionCommandArgument;
import com.eightkdata.mongowp.server.api.impl.NonWriteCommandResult;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.mongodb.commands.AbstractToroCommandImplementation;

/**
 *
 */
public class DropCollectionImplementation extends
        AbstractToroCommandImplementation<CollectionCommandArgument, Empty> {

    public static final DropCollectionImplementation INSTANCE = new DropCollectionImplementation();

    @Override
    public CommandResult<Empty> apply(
            Command<? super CollectionCommandArgument, ? super Empty> command,
            CommandRequest<CollectionCommandArgument> req) throws MongoException {

        String collection = req.getCommandArgument().getCollection();
        ToroConnection connection = getToroConnection(req);

        if (collection == null) {
            throw new BadValueException("No collection name specified");
        }
        connection.dropCollection(collection);

        return new NonWriteCommandResult<>(Empty.getInstance());

    }

}
