
package com.torodb.torod.mongodb;

import com.eightkdata.mongowp.mongoserver.api.safe.*;
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
import com.torodb.torod.core.pojos.NamedToroIndex;
import com.torodb.torod.core.subdocument.ToroDocument;
import com.torodb.torod.mongodb.meta.commands.MetaCollStatsImplementation;
import com.torodb.torod.mongodb.translator.KVToroDocument;
import com.torodb.torod.mongodb.translator.QueryCriteriaTranslator;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import javax.inject.Inject;

/**
 *
 */
public class ToroNamespacesSubRequestProcessor extends AbstractMetaSubRequestProcessor {

    private final CommandsLibrary commandsLibrary;
    private final CommandsExecutor commandsExecutor;

    @Inject
    public ToroNamespacesSubRequestProcessor(
            @DatabaseName String databaseName,
            BuildProperties buildProperties,
            QueryCriteriaTranslator queryCriteriaTranslator,
            OptimeClock optimeClock) {
        super("system.namespaces", databaseName, queryCriteriaTranslator, optimeClock);

        commandsLibrary = new NameBasedCommandsLibrary(
                "toro-" + buildProperties.getFullVersion() + "-namespaces",
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
    public List<ToroDocument> queryAllDocuments(Connection connection) {
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

                candidates.add(
                        new KVToroDocument(
                                new ObjectValue.Builder()
                                .putValue("name", collectionNamespace)
                                .build()
                        )
                );

                Collection<? extends NamedToroIndex> indexes
                        = transaction.getIndexes(collection);
                for (NamedToroIndex index : indexes) {
                    candidates.add(
                            new KVToroDocument(
                                    new ObjectValue.Builder()
                                    .putValue("name", collectionNamespace + ".$"
                                            + index.getName())
                                    .build()
                            )
                    );
                }
            }
            candidates.add(
                    new KVToroDocument(
                            new ObjectValue.Builder()
                            .putValue("name", databaseName + ".system.indexes")
                            .build()
                    )
            );

            return candidates;
        }
        catch (ImplementationDbException ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        } finally {
            if (transaction != null) {
                transaction.close();
            }
        }


    }

}
