package com.torodb.mongowp.mongoserver.api.toro;

import com.eightkdata.mongowp.mongoserver.api.MetaCommandProcessor;
import com.eightkdata.mongowp.mongoserver.api.commands.pojos.CollStatsReply;
import com.eightkdata.mongowp.mongoserver.api.commands.pojos.CollStatsRequest;
import com.eightkdata.mongowp.mongoserver.api.commands.pojos.InsertReply;
import com.eightkdata.mongowp.mongoserver.protocol.MongoWP;
import com.eightkdata.nettybson.api.BSONDocument;
import com.eightkdata.nettybson.mongodriver.MongoBSONDocument;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import com.mongodb.WriteConcern;
import com.torodb.kvdocument.values.*;
import com.torodb.mongowp.mongoserver.api.toro.util.BSONDocuments;
import com.torodb.mongowp.mongoserver.api.toro.util.KVToroDocument;
import com.torodb.torod.core.annotations.DatabaseName;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.core.connection.ToroTransaction;
import com.torodb.torod.core.exceptions.UserToroException;
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.language.querycriteria.*;
import com.torodb.torod.core.pojos.IndexedAttributes;
import com.torodb.torod.core.pojos.NamedToroIndex;
import com.torodb.torod.core.subdocument.ToroDocument;
import com.torodb.torod.core.utils.DocValueQueryCriteriaEvaluator;
import com.torodb.translator.QueryCriteriaTranslator;
import io.netty.util.AttributeMap;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nonnull;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

/**
 *
 */
public class ToroMetaCommandProcessor extends MetaCommandProcessor {

    private final String databaseName;
    private final QueryCriteriaTranslator queryCriteriaTranslator;
    private final ToroQueryCommandProcessor commandProcesor;

    @Inject
    public ToroMetaCommandProcessor(
            @DatabaseName String databaseName,
            @Nonnull QueryCriteriaTranslator queryCriteriaTranslator,
            @Nonnull ToroQueryCommandProcessor commandProcessor) {
        this.databaseName = databaseName;
        this.queryCriteriaTranslator = queryCriteriaTranslator;
        this.commandProcesor = commandProcessor;
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
    public Future<InsertReply> insertIndex(
            AttributeMap attributeMap, 
            List<BSONDocument> docsToInsert, 
            boolean ordered, 
            WriteConcern wc) throws Exception {        
        //TODO: right now it works as all or nothing command... and if it is nothing, a ugly error is thrown

        String collection;
        Future<?> commitFutureTemp = null;
        final List<Future<?>> indexCreationFutures = Lists.newArrayListWithCapacity(docsToInsert.size());

        for (BSONDocument index : docsToInsert) {
            collection = (String) index.getValue("ns");
            assert collection != null;
            assert collection.indexOf('.') >= 0;
            collection = collection.substring(collection.indexOf('.') + 1);

            ToroConnection connection
                    = attributeMap.attr(ToroRequestProcessor.CONNECTION).get();

            ToroTransaction transaction = null;
            try {
                
                transaction = connection.createTransaction();
                for (BSONDocument doc : docsToInsert) {
                    BSONObject uncastedIndex
                            = new BasicBSONObject(doc.asMap());

                    String name = (String) uncastedIndex.removeField("name");
                    BSONObject key
                            = (BSONObject) uncastedIndex.removeField("key");
                    Boolean unique
                            = (Boolean) uncastedIndex.removeField("unique");
                    unique = unique != null ? unique : false;
                    Boolean sparse
                            = (Boolean) uncastedIndex.removeField("sparse");
                    sparse = sparse != null ? sparse : false;

                    uncastedIndex.removeField("ns");
                    uncastedIndex.removeField("_id");
                    uncastedIndex.removeField("v");
                    
                    
                    if (!uncastedIndex.keySet().isEmpty()) {
                        throw new IllegalArgumentException("Options "
                                + uncastedIndex.keySet().toString()
                                + " are not supported");
                    }

                    IndexedAttributes.Builder indexedAttsBuilder
                            = new IndexedAttributes.Builder();

                    for (String path : key.keySet()) {
                        AttributeReference attRef
                                = parseAttributeReference(path);
                        boolean ascending
                                = ((Number) key.get(path)).intValue()
                                > 0;

                        indexedAttsBuilder.addAttribute(attRef, ascending);
                    }

                    indexCreationFutures.add(
                            transaction.createIndex(
                                    collection,
                                    name,
                                    indexedAttsBuilder.build(),
                                    unique,
                                    sparse
                            )
                    );
                }
                commitFutureTemp = transaction.commit();
            }
            catch (Throwable ex) {
                commitFutureTemp = Futures.immediateFailedFuture(ex);
            }
            finally {
                if (transaction != null) {
                    transaction.close();
                }
            }
        }
        
        final Future<?> commitFuture = commitFutureTemp;

        return new InsertResponseFuture(indexCreationFutures, commitFuture);
    }

    private AttributeReference parseAttributeReference(String path) {
        AttributeReference.Builder builder = new AttributeReference.Builder();

        StringTokenizer st = new StringTokenizer(path, ".");
        while (st.hasMoreTokens()) {
            builder.addObjectKey(st.nextToken());
        }
        return builder.build();
    }

    @Override
    public Future<InsertReply> insertNamespace(
            AttributeMap attributeMap, 
            List<BSONDocument> docsToInsert, 
            boolean ordered, WriteConcern wc) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Future<InsertReply> insertProfile(
            AttributeMap attributeMap, 
            List<BSONDocument> docsToInsert, 
            boolean ordered, 
            WriteConcern wc) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Future<InsertReply> insertJS(
            AttributeMap attributeMap, 
            List<BSONDocument> docsToInsert, 
            boolean ordered, 
            WriteConcern wc) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
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

    private static class InsertResponseFuture implements Future<InsertReply> {

        private final List<Future<?>> creationFutures;
        private final Future<?> commitFuture;

        public InsertResponseFuture(List<Future<?>> creationFutures, Future<?> commitFuture) {
            this.creationFutures = creationFutures;
            this.commitFuture = commitFuture;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            throw new UnsupportedOperationException("Not supported");
        }

        @Override
        public boolean isCancelled() {
            return false;
        }
        
        @Override
        public boolean isDone() {
            for (Future<?> indexCreationFuture : creationFutures) {
                if (!indexCreationFuture.isDone()) {
                    return false;
                }
            }
            return commitFuture.isDone();
        }
        
        @Override
        public InsertReply get() throws InterruptedException,
                ExecutionException {
            ImmutableList.Builder<InsertReply.WriteError> writeErrorsBuilder
                    = ImmutableList.builder();
            for (int i = 0; i < creationFutures.size(); i++) {
                try {
                    creationFutures.get(i).get();
                }
                catch (Throwable ex) {
                    writeErrorsBuilder.add(new InsertReply.WriteError(
                                    i,
                                    MongoWP.ErrorCode.INTERNAL_ERROR.getErrorCode(),
                                    ex.getMessage()
                            )
                    );
                }
            }
            ImmutableList<InsertReply.WriteError> writeErrors
                    = writeErrorsBuilder.build();
            return new ToroInsertReply(
                    writeErrors != null,
                    creationFutures.size() - writeErrors.size(),
                    writeErrors,
                    ImmutableList.<InsertReply.WriteConcernError>of()
            );
        }
        
        @Override
        public InsertReply get(long timeout, TimeUnit unit) throws
                InterruptedException, ExecutionException,
                TimeoutException {
            throw new UnsupportedOperationException("Not supported.");
        }
    }
    
    
}
