
package com.torodb.mongodb.commands.impl.admin;

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.bson.utils.DefaultBsonValues;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.ListCollectionsCommand.ListCollectionsArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.ListCollectionsCommand.ListCollectionsResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.ListCollectionsCommand.ListCollectionsResult.Entry;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.CollectionOptions;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.CollectionOptions.AutoIndexMode;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.CursorResult;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.torodb.mongodb.commands.impl.ReadTorodbCommandImpl;
import com.torodb.mongodb.core.MongodTransaction;
import javax.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
@Singleton
public class ListCollectionsImplementation implements ReadTorodbCommandImpl<ListCollectionsArgument, ListCollectionsResult> {

    private static final Logger LOGGER = LogManager.getLogger(ListCollectionsImplementation.class);
    public static final String LIST_COLLECTIONS_GET_MORE_COLLECTION = "$cmd.listCollections";

    private static final CollectionOptions DEFAULT_COLLECTION_OPTIONS = new CollectionOptions.Builder()
            .setAutoIndexMode(AutoIndexMode.DEFAULT)
            .setCapped(false)
            .setStorageEngine(DefaultBsonValues.newDocument("engine", DefaultBsonValues.newString("torodb")))
            .setTemp(false)
            .build();

    @Override
    public Status<ListCollectionsResult> apply(Request req, Command<? super ListCollectionsArgument, ? super ListCollectionsResult> command,
            ListCollectionsArgument arg, MongodTransaction context) {

        if (arg.getFilter() != null && !arg.getFilter().isEmpty()) {
            LOGGER.debug("Recived a {} with the unsupported filter {}", command.getCommandName(), arg.getFilter());
            return Status.from(ErrorCode.COMMAND_FAILED, command.getCommandName() + " with filters are not supported right now");
        }

        return Status.ok(
                new ListCollectionsResult(
                        CursorResult.createSingleBatchCursor(req.getDatabase(), LIST_COLLECTIONS_GET_MORE_COLLECTION, 
                                context.getTorodTransaction().getCollectionsInfo().map(colInfo -> 
                                        new Entry(colInfo.getName(), DEFAULT_COLLECTION_OPTIONS)
                                )
                        )
                )
        );

    }



}
