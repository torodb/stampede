
package com.torodb.mongodb.commands.impl.diagnostic;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.bson.utils.DefaultBsonValues;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.CollStatsCommand.CollStatsArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.CollStatsCommand.CollStatsReply;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.torodb.core.exceptions.user.CollectionNotFoundException;
import com.torodb.core.model.NamedToroIndex;
import com.torodb.mongodb.commands.impl.ReadTorodbCommandImpl;
import com.torodb.mongodb.core.MongodTransaction;
import com.torodb.mongodb.language.utils.NamespaceUtil;
import com.torodb.torod.CollectionInfo;

/**
 *
 */
public class CollStatsImplementation implements ReadTorodbCommandImpl<CollStatsArgument, CollStatsReply>{

    private static final Logger LOGGER
            = LogManager.getLogger(CollStatsImplementation.class);

    @Override
    public Status<CollStatsReply> apply(Request req, Command<? super CollStatsArgument, ? super CollStatsReply> command,
            CollStatsArgument arg, MongodTransaction context) {

        String collection = arg.getCollection();
        CollStatsReply.Builder replyBuilder = new CollStatsReply.Builder(
                req.getDatabase(),
                collection
        );
        if (NamespaceUtil.isSystem(collection)) {
            //TODO (matteom): support stats on system collections
            LOGGER.warn("Requested stats on the system collection "
                    + collection + ". ToroDB does not support stats for system "
                    + "collections yet");
            Stream<CollectionInfo> collectionsInfo = context.getTorodTransaction().getCollectionsInfo(
                    req.getDatabase());
            replyBuilder.setCount(collectionsInfo.count())
                    .setSize(0)
                    .setStorageSize(0)
                    .setCustomStorageStats(null)
                    .setIndexDetails(DefaultBsonValues.EMPTY_DOC)
                    .setScale(arg.getScale())
                    .setSizeByIndex(Collections.<String, Number>emptyMap())
                    .setCapped(false);
        }
        else {
            try {
                CollectionInfo collectionInfo = context.getTorodTransaction().getCollectionInfo(
                        req.getDatabase(), arg.getCollection());
                if (collectionInfo.isCapped()) {
                    replyBuilder.setCapped(true)
                            .setMaxIfCapped(collectionInfo.getMaxIfCapped());
                } else {
                    replyBuilder.setCapped(false);
                }
            } catch (CollectionNotFoundException e) {
            }
            replyBuilder
                    .setCapped(false)
                    .setScale(arg.getScale());

            int scale = replyBuilder.getScale();
            //TODO (matteom): add index stats
            Collection<? extends NamedToroIndex> indexes
                    = ImmutableList.of();
            Map<String, Long> sizeByMap = Maps.newHashMapWithExpectedSize(indexes.size());

            replyBuilder.setSizeByIndex(sizeByMap);

            replyBuilder.setCount(
                    context.getTorodTransaction().countAll(
                            req.getDatabase(), collection
                    )
            );
            replyBuilder.setSize(
                    context.getTorodTransaction().getDocumentsSize(
                            req.getDatabase(), collection
                    ) / scale
            );
            replyBuilder.setStorageSize(
                    context.getTorodTransaction().getCollectionSize(
                            req.getDatabase(), collection
                    ) / scale
            );
        }
        
        return Status.ok(replyBuilder.build());
    }

}
