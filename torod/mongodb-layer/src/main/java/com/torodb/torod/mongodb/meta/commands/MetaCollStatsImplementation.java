
package com.torodb.torod.mongodb.meta.commands;

import com.eightkdata.mongowp.mongoserver.api.safe.Command;
import com.eightkdata.mongowp.mongoserver.api.safe.CommandRequest;
import com.eightkdata.mongowp.mongoserver.api.safe.CommandImplementation;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.CollStatsCommand.CollStatsArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.CollStatsCommand.CollStatsReply;
import com.torodb.torod.core.subdocument.ToroDocument;
import com.torodb.torod.mongodb.AbstractMetaSubRequestProcessor;
import java.util.Collections;
import java.util.List;
import org.bson.BsonDocument;

/**
 *
 */
public class MetaCollStatsImplementation implements CommandImplementation<CollStatsArgument, CollStatsReply>{

    private final AbstractMetaSubRequestProcessor subRequestProcessor;

    public MetaCollStatsImplementation(AbstractMetaSubRequestProcessor subRequestProcessor) {
        this.subRequestProcessor = subRequestProcessor;
    }

    @Override
    public CollStatsReply apply(Command<? extends CollStatsArgument, ? extends CollStatsReply> command, CommandRequest<CollStatsArgument> req) {
        CollStatsArgument arg = req.getCommandArgument();

        List<ToroDocument> allDocs
                = subRequestProcessor.queryAllDocuments(req.getConnection());

        return new CollStatsReply.Builder(command, req.getDatabase(), arg.getCollection())
                .setCapped(false)
                .setCount(allDocs.size())
                .setCustomStorageStats(null)
                .setIndexDetails(new BsonDocument())
                .setScale(arg.getScale())
                .setSize(0)
                .setSizeByIndex(Collections.<String, Number>emptyMap())
                .setStorageSize(0)
                .build();
    }

}
