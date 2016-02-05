
package com.torodb.torod.mongodb.commands.impl.admin;

import com.eightkdata.mongowp.exceptions.BadValueException;
import com.eightkdata.mongowp.exceptions.CommandFailed;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandRequest;
import com.eightkdata.mongowp.server.api.CommandResult;
import com.eightkdata.mongowp.server.api.impl.NonWriteCommandResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.CreateCollectionCommand.CreateCollectionArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.CollectionOptions;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.CollectionOptions.AutoIndexMode;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.mongodb.commands.AbstractToroCommandImplementation;
import com.torodb.torod.mongodb.utils.JsonToBson;
import javax.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class CreateCollectionImplementation extends AbstractToroCommandImplementation<CreateCollectionArgument, Empty> {

    private static final Logger LOGGER
            = LoggerFactory.getLogger(CreateCollectionImplementation.class);
    public static final CreateCollectionImplementation INSTANCE = new CreateCollectionImplementation();

    @Override
    public CommandResult<Empty> apply(Command<? super CreateCollectionArgument, ? super Empty> command, CommandRequest<CreateCollectionArgument> req)
            throws MongoException {
        CreateCollectionArgument arg = req.getCommandArgument();
        String collection = arg.getCollection();
        CollectionOptions options = arg.getOptions();
        ToroConnection connection = getToroConnection(req);

        if (options.isCapped()) {
            throw new CommandFailed(command.getCommandName(), "Capped collections are not supported on ToroDB");
        }
        if (collection == null) {
            throw new BadValueException("No collection name specified");
        }
        if (options.isTemp()) {
            LOGGER.warn("Ignored 'temp' option while creating the collection {}", collection);
        }

        if (options.getAutoIndexMode().equals(AutoIndexMode.NO)) {
            LOGGER.warn("Ingored 'autoIndexId' while creating the collection {}", collection);
        }

        JsonObject other;
        if (options.getStorageEngine() == null) {
            other = null;
        }
        else {
            other = JsonToBson.transform(options.getStorageEngine());
        }

        connection.createCollection(collection, other);
        connection.dropCollection(collection);

        return new NonWriteCommandResult<>(Empty.getInstance());

    }


}
