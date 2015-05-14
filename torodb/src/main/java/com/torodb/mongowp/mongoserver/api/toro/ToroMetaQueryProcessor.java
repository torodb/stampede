package com.torodb.mongowp.mongoserver.api.toro;

import com.eightkdata.mongowp.mongoserver.api.MetaQueryProcessor;
import com.eightkdata.mongowp.mongoserver.api.commands.CollStatsReply;
import com.eightkdata.mongowp.mongoserver.api.commands.CollStatsRequest;
import com.eightkdata.nettybson.api.BSONDocument;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.torodb.kvdocument.values.*;
import com.torodb.mongowp.mongoserver.api.toro.util.BSONDocuments;
import com.torodb.mongowp.mongoserver.api.toro.util.KVToroDocument;
import com.torodb.torod.core.annotations.DatabaseName;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.core.connection.ToroTransaction;
import com.torodb.torod.core.exceptions.UserToroException;
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.language.querycriteria.*;
import com.torodb.torod.core.pojos.NamedToroIndex;
import com.torodb.torod.core.subdocument.ToroDocument;
import com.torodb.torod.core.utils.DocValueQueryCriteriaEvaluator;
import com.torodb.translator.QueryCriteriaTranslator;
import io.netty.util.AttributeMap;
import java.util.*;
import org.bson.BSONObject;

/**
 *
 */
public class ToroMetaQueryProcessor extends MetaQueryProcessor {

    private final String databaseName;
    private final QueryCriteriaTranslator queryCriteriaTranslator;

    @Inject
    public ToroMetaQueryProcessor(
            @DatabaseName String databaseName,
            QueryCriteriaTranslator queryCriteriaTranslator) {
        this.databaseName = databaseName;
        this.queryCriteriaTranslator = queryCriteriaTranslator;
    }

    @Override
    protected Iterable<BSONDocument> queryNamespaces(
            String database, 
            AttributeMap attributeMap, 
            BSONObject query)
            throws Exception {
        
        if (!database.equals(databaseName)) {
            return Collections.emptyList();
        }
        
        ToroConnection connection
                = attributeMap.attr(ToroRequestProcessor.CONNECTION).get();
        
        Collection<String> allCollections = connection.getCollections();

        List<ToroDocument> candidates = Lists.newArrayList();
        ToroTransaction transaction = connection.createTransaction();
        try {

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

        }
        finally {
            transaction.close();
        }

        QueryCriteria queryCriteria;
        if (query == null) {
            queryCriteria = TrueQueryCriteria.getInstance();
        } else {
            queryCriteria = queryCriteriaTranslator.translate(query);
        }
        Collection<ToroDocument> filtrered
                = applyQueryCriteria(candidates, queryCriteria);

        return new BSONDocuments(filtrered);
    }

    @Override
    protected Iterable<BSONDocument> queryIndexes(
            String database,
            AttributeMap attributeMap, 
            BSONObject query)
            throws Exception {
        
        if (!database.equals(databaseName)) {
            return Collections.emptyList();
        }
        
        ToroConnection connection
                = attributeMap.attr(ToroRequestProcessor.CONNECTION).get();
        Collection<String> allCollections = connection.getCollections();

        List<ToroDocument> candidates = Lists.newArrayList();
        ToroTransaction transaction = connection.createTransaction();
        try {
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

        }
        finally {
            transaction.close();
        }

        QueryCriteria queryCriteria;
        if (query == null) {
            queryCriteria = TrueQueryCriteria.getInstance();
        } else {
            queryCriteria = queryCriteriaTranslator.translate(query);
        }
        Collection<ToroDocument> filtrered
                = applyQueryCriteria(candidates, queryCriteria);

        return new BSONDocuments(filtrered);
    }

    @Override
    protected Iterable<BSONDocument> queryProfile(
            String database,
            AttributeMap attributeMap, 
            BSONObject query)
            throws Exception {
        
        return Collections.emptySet();
    }

    @Override
    protected Iterable<BSONDocument> queryJS(
            String database,
            AttributeMap attributeMap, 
            BSONObject query)
            throws Exception {
        return Collections.emptySet();
    }

    @Override
    public CollStatsReply collStats(
            String database,
            CollStatsRequest request, 
            Supplier<Iterable<BSONDocument>> docsSupplier) throws Exception {
        
        if (!database.equals(databaseName)) {
            throw new UserToroException(
                    "Collection [" + database + '.' + request.getCollection() 
                    + "] not found. ");
        }
        CollStatsReply.Builder replyBuilder = new CollStatsReply.Builder(
                request.getDatabase(), 
                request.getCollection())
                .setCapped(false)
                .setIdIndexExists(false)
                .setLastExtentSize(1)
                .setNumExtents(1)
                .setPaddingFactor(0)
                .setScale(request.getScale().intValue())
                .setSize(0)
                .setSizeByIndex(Collections.<String, Number>emptyMap())
                .setStorageSize(0)
                .setUsePowerOf2Sizes(false);
        
        replyBuilder.setCount(Iterables.size(docsSupplier.get()));
        
        return replyBuilder.build();
    }

    private Collection<ToroDocument> applyQueryCriteria(
            Collection<ToroDocument> candidates,
            QueryCriteria queryCriteria) {

        
        QCEvaluatorPredicate predicate = new QCEvaluatorPredicate(queryCriteria);

        return Collections2.filter(candidates, predicate);
    }
    
    private static class QCEvaluatorPredicate implements Predicate<ToroDocument> {

        private final QueryCriteria qc;

        public QCEvaluatorPredicate(QueryCriteria qc) {
            this.qc = qc;
        }
        
        @Override
        public boolean apply(ToroDocument input) {
            if (input == null) {
                throw new IllegalArgumentException("The document to filter must be not null");
            }
            return DocValueQueryCriteriaEvaluator.evaluate(qc, input.getRoot());
        }
        
    }
    
    
}
