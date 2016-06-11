
package com.torodb.torod.mongodb.commands.impl.diagnostic;

import com.eightkdata.mongowp.bson.utils.DefaultBsonValues;
import com.eightkdata.mongowp.exceptions.CommandFailed;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandRequest;
import com.eightkdata.mongowp.server.api.CommandResult;
import com.eightkdata.mongowp.server.api.impl.NonWriteCommandResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.CollStatsCommand.CollStatsArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.CollStatsCommand.CollStatsReply;
import com.google.common.collect.Maps;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.core.connection.ToroTransaction;
import com.torodb.torod.core.connection.TransactionMetainfo;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.language.querycriteria.TrueQueryCriteria;
import com.torodb.torod.core.pojos.NamedToroIndex;
import com.torodb.torod.mongodb.commands.AbstractToroCommandImplementation;
import com.torodb.torod.mongodb.meta.MetaCollection;
import com.torodb.torod.mongodb.meta.MetaCollectionProvider;
import com.torodb.torod.mongodb.utils.NamespaceUtil;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class CollStatsImplementation extends AbstractToroCommandImplementation<CollStatsArgument, CollStatsReply>{

    private static final Logger LOGGER
            = LoggerFactory.getLogger(CollStatsImplementation.class);
    private final MetaCollectionProvider metaCollectionProvider;

    @Inject
    public CollStatsImplementation(MetaCollectionProvider metaCollectionProvider) {
        this.metaCollectionProvider = metaCollectionProvider;
    }

    @Override
    public CommandResult<CollStatsReply> apply(
            Command<? super CollStatsArgument, ? super CollStatsReply> command,
            CommandRequest<CollStatsArgument> req) throws MongoException {

        CollStatsArgument arg = req.getCommandArgument();

        String collection = arg.getCollection();
        CollStatsReply.Builder replyBuilder = new CollStatsReply.Builder(
                req.getDatabase(),
                collection
        );
        if (NamespaceUtil.isSystem(collection)) {
            //TODO (gortiz): support stats on system collections
            LOGGER.warn("Requested stats on the system collection "
                    + collection + ". ToroDB does not support stats for system "
                    + "collections yet");
            MetaCollection metaCollection = metaCollectionProvider.getMetaCollection(
                    req.getDatabase(),
                    collection
            );
            replyBuilder.setCount(metaCollection.count(getToroConnection(req)))
                    .setSize(0)
                    .setStorageSize(0)
                    .setCustomStorageStats(null)
                    .setIndexDetails(DefaultBsonValues.EMPTY_DOC)
                    .setScale(arg.getScale())
                    .setSizeByIndex(Collections.<String, Number>emptyMap());
            if (metaCollection.isCapped()) {
                replyBuilder.setCapped(true)
                        .setMaxIfCapped(metaCollection.getMaxIfCapped());
            }
            else {
                replyBuilder.setCapped(false);
            }
        }
        else {
            replyBuilder
                    .setCapped(false)
                    .setScale(arg.getScale());

            ToroConnection connection = getToroConnection(req);

            try {
                int scale = replyBuilder.getScale();
                try (ToroTransaction transaction = connection.createTransaction(TransactionMetainfo.READ_ONLY)) {
                    Collection<? extends NamedToroIndex> indexes
                            = transaction.getIndexes(collection);
                    Map<String, Long> sizeByMap = Maps.newHashMapWithExpectedSize(indexes.size());
                    for (NamedToroIndex index : indexes) {
                        Long indexSize = transaction.getIndexSize(
                                collection,
                                index.getName()
                        ).get();
                        sizeByMap.put(index.getName(), indexSize / scale);
                    }

                    replyBuilder.setSizeByIndex(sizeByMap);

                    replyBuilder.setCount(
                            transaction.count(
                                    collection,
                                    TrueQueryCriteria.getInstance()
                            ).get()
                    );
                    replyBuilder.setSize(
                            transaction.getDocumentsSize(
                                    collection
                            ).get() / scale
                    );
                    replyBuilder.setStorageSize(
                            transaction.getCollectionSize(
                                    collection
                            ).get() / scale
                    );
                } catch (InterruptedException ex) {
                    throw new CommandFailed(command.getCommandName(), ex.getMessage(), ex);
                } catch (ExecutionException ex) {
                    throw new CommandFailed(command.getCommandName(), ex.getMessage(), ex);
                }
            } catch (ImplementationDbException ex) {
                throw new CommandFailed(command.getCommandName(), ex.getMessage(), ex);
            }
        }
        
        return new NonWriteCommandResult<>(replyBuilder.build());
    }

}
