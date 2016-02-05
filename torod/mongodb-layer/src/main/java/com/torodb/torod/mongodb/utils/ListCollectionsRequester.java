
package com.torodb.torod.mongodb.utils;

import com.eightkdata.mongowp.MongoVersion;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.client.core.MongoConnection;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.server.api.pojos.MongoCursor;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.ListCollectionsCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.ListCollectionsCommand.ListCollectionsResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.ListCollectionsCommand.ListCollectionsResult.Entry;
import javax.annotation.Nullable;

/**
 * Utility class to get the collections metadata in a version independient way.
 * <p/>
 * The command {@link ListCollectionsCommand listCollections} is only available
 * since {@linkplain MongoVersion#V3_0 MongoDB 3.0}. In previous versions, a
 * query to an specific metacollection must be done.
 * <p/>
 * This class is used request for collections metadata in a version independient
 * way.
 */
public class ListCollectionsRequester {

    private ListCollectionsRequester() {}

    public static MongoCursor<Entry> getListCollections(
            MongoConnection connection,
            String database,
            @Nullable BsonDocument filter
    ) throws MongoException {
        boolean commandSupported = connection.getClientOwner()
                .getMongoVersion().compareTo(MongoVersion.V3_0) >= 0;
        if (commandSupported) {
            return getFromCommand(connection, database, filter);
        }
        else {
            return getFromQuery(connection, database, filter);
        }
    }

    private static MongoCursor<Entry> getFromCommand(
            MongoConnection connection,
            String database,
            @Nullable BsonDocument filter
    ) throws MongoException {
        ListCollectionsResult reply = connection.execute(
                ListCollectionsCommand.INSTANCE,
                database,
                true,
                new ListCollectionsCommand.ListCollectionsArgument(
                        filter
                )
        );
        return reply.getCursor();
    }

    private static MongoCursor<Entry> getFromQuery(
            MongoConnection connection,
            String database,
            BsonDocument filter) {
        throw new UnsupportedOperationException("Not supported yet");
    }

}
