
package com.torodb.torod.mongodb.meta;

import com.google.common.collect.Lists;
import com.torodb.kvdocument.values.ObjectValue;
import com.torodb.torod.core.annotations.DatabaseName;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.core.connection.ToroTransaction;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.pojos.NamedToroIndex;
import com.torodb.torod.core.subdocument.ToroDocument;
import com.torodb.torod.mongodb.translator.KVToroDocument;
import com.torodb.torod.mongodb.utils.NamespaceUtil;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 *
 */
@Singleton
public class IndexesMetaCollection extends MetaCollection {

    @Inject
    public IndexesMetaCollection(@DatabaseName String databaseName) {
        super(databaseName, NamespaceUtil.INDEXES_COLLECTION);
    }

    @Override
    public List<ToroDocument> queryAllDocuments(ToroConnection toroConnection)
            throws RuntimeException {

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

    @Override
    public long count(ToroConnection toroConnection) {
        Collection<String> allCollections = toroConnection.getCollections();

        ToroTransaction transaction = null;
        long count = 0;
        try {
            transaction = toroConnection.createTransaction();
            for (String collection : allCollections) {

                count += transaction.getIndexes(collection).size();
            }

            return count;
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

    @Override
    public boolean isCapped() {
        return false;
    }

    @Override
    public Number getMaxIfCapped() {
        return null;
    }
}
