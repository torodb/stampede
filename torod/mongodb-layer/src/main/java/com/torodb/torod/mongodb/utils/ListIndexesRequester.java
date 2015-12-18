
package com.torodb.torod.mongodb.utils;

import com.eightkdata.mongowp.client.core.MongoConnection;
import com.eightkdata.mongowp.mongoserver.MongoVersion;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.ListIndexesCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.ListIndexesCommand.ListIndexesResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.IndexOptions;
import com.eightkdata.mongowp.mongoserver.api.safe.pojos.MongoCursor;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.MongoException;

/**
 * Utility class to get the indexes metadata in a version independient way.
 * <p/>
 * The command {@link ListIndexesCommand listIndexes} is only available
 * since {@linkplain MongoVersion#V3_0 MongoDB 3.0}. In previous versions, a
 * query to an specific metacollection must be done.
 * <p/>
 * This class is used request for collections metadata in a version independient
 * way.
 */
public class ListIndexesRequester {

    private ListIndexesRequester() {}

    public static MongoCursor<IndexOptions> getListCollections(
            MongoConnection connection,
            String database,
            String collection
    ) throws MongoException {
        boolean commandSupported = connection.getClientOwner()
                .getMongoVersion().compareTo(MongoVersion.V3_0) >= 0;
        if (commandSupported) {
            return getFromCommand(connection, database, collection);
        }
        else {
            return getFromQuery(connection, database, collection);
        }
    }

    private static MongoCursor<IndexOptions> getFromCommand(
            MongoConnection connection,
            String database,
            String collection) throws MongoException {
        ListIndexesResult reply = connection.execute(
                ListIndexesCommand.INSTANCE,
                database,
                true,
                new ListIndexesCommand.ListIndexesArgument(
                        collection
                )
        );
        return reply.getCursor();
    }

    private static MongoCursor<IndexOptions> getFromQuery(
            MongoConnection connection,
            String database,
            String collection) {
        throw new UnsupportedOperationException("Not supported yet");
    }
}
