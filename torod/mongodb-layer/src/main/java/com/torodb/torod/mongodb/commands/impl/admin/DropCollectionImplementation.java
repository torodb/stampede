
package com.torodb.torod.mongodb.commands.impl.admin;

import com.eightkdata.mongowp.mongoserver.api.safe.Command;
import com.eightkdata.mongowp.mongoserver.api.safe.CommandRequest;
import com.eightkdata.mongowp.mongoserver.api.safe.CommandResult;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.CollectionCommandArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.NonWriteCommandResult;
import com.eightkdata.mongowp.mongoserver.api.safe.tools.Empty;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.BadValueException;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.MongoException;
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

        return new NonWriteCommandResult<Empty>(Empty.getInstance());

    }

}
