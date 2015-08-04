
package com.torodb.torod.mongodb;

import com.eightkdata.mongowp.mongoserver.api.safe.Command;
import com.eightkdata.mongowp.mongoserver.api.safe.CommandsExecutor;
import com.eightkdata.mongowp.mongoserver.api.safe.CommandsLibrary;
import com.eightkdata.mongowp.mongoserver.api.safe.Connection;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.MapBasedCommandsExecutor;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.NameBasedCommandsLibrary;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.CollStatsCommand;
import com.google.common.collect.Lists;
import com.torodb.kvdocument.values.ObjectValue;
import com.torodb.torod.core.BuildProperties;
import com.torodb.torod.core.annotations.DatabaseName;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.core.connection.ToroTransaction;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.pojos.NamedToroIndex;
import com.torodb.torod.core.subdocument.ToroDocument;
import com.torodb.torod.mongodb.meta.commands.MetaCollStatsImplementation;
import com.torodb.torod.mongodb.translator.KVToroDocument;
import com.torodb.torod.mongodb.translator.QueryCriteriaTranslator;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;

/**
 *
 */
public class ToroIndexSubRequestProcessor extends AbstractMetaSubRequestProcessor {

    private final CommandsLibrary commandsLibrary;
    private final CommandsExecutor commandsExecutor;

    @Inject
    public ToroIndexSubRequestProcessor(
            @DatabaseName String databaseName,
            BuildProperties buildProperties,
            QueryCriteriaTranslator queryCriteriaTranslator,
            OptimeClock optimeClock) {
        super(
                "system.indexes",
                databaseName,
                queryCriteriaTranslator,
                optimeClock
        );

        commandsLibrary = new NameBasedCommandsLibrary(
                "toro-" + buildProperties.getFullVersion() + "-index",
                Collections.<Command>singleton(
                        CollStatsCommand.INSTANCE
                )
        );
        commandsExecutor = MapBasedCommandsExecutor.fromLibraryBuilder(commandsLibrary)
                .addImplementation(CollStatsCommand.INSTANCE, new MetaCollStatsImplementation(this))
                .build();
    }

    @Override
    protected CommandsExecutor getCommandsExecutor() {
        return commandsExecutor;
    }

    @Override
    public CommandsLibrary getCommandsLibrary() {
        return commandsLibrary;
    }

    @Override
    public List<ToroDocument> queryAllDocuments(Connection connection)
            throws RuntimeException {

        ToroConnection toroConnection =
                RequestContext.getFrom(connection.getAttributeMap())
                .getToroConnection();
        Collection<String> allCollections = toroConnection.getCollections();

        List<ToroDocument> candidates = Lists.newArrayList();
        ToroTransaction transaction = null;
        String databaseName = getDatabaseName();
        try {
            transaction = toroConnection.createTransaction();
            for (String collection : allCollections) {

                String collectionNamespace = databaseName + '.' + collection;

                Collection<? extends NamedToroIndex> indexes
                        = transaction.getIndexes(collection);
                for (NamedToroIndex index : indexes) {
                    ObjectValue.Builder objBuider = new ObjectValue.Builder()
                            .putValue("v", 1)
                            .putValue("name", index.getName())
                            .putValue("ns", collectionNamespace)
                            .putValue("key", new ObjectValue.Builder()
                            );
                    ObjectValue.Builder keyBuilder = new ObjectValue.Builder();
                    for (Map.Entry<AttributeReference, Boolean> entrySet : index.getAttributes().entrySet()) {
                        keyBuilder.putValue(
                                entrySet.getKey().toString(),
                                entrySet.getValue() ? 1 : -1
                        );
                    }
                    objBuider.putValue("key", keyBuilder);

                    candidates.add(
                            new KVToroDocument(
                                    objBuider.build()
                            )
                    );
                }
            }

            return candidates;
        }
        catch (ImplementationDbException ex) {
            throw new RuntimeException(ex.getLocalizedMessage(), ex);
        }
        finally {
            if (transaction != null) {
                transaction.close();
            }
        }
    }

}
