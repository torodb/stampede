
package com.torodb.torod.mongodb.meta;

import com.eightkdata.mongowp.mongoserver.protocol.exceptions.DatabaseNotFoundException;
import com.torodb.torod.core.annotations.DatabaseName;
import com.torodb.torod.mongodb.utils.NamespaceUtil;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@Singleton
public class MetaCollectionProvider {

    private static final Logger LOGGER
            = LoggerFactory.getLogger(MetaCollectionProvider.class);
    private final String supportedDatabase;
    private final IndexesMetaCollection indexes;
    private final NamespacesMetaCollection namespaces;

    @Inject
    public MetaCollectionProvider(
            @DatabaseName String supportedDatabase,
            IndexesMetaCollection indexes,
            NamespacesMetaCollection namespaces) {
        this.supportedDatabase = supportedDatabase;
        this.indexes = indexes;
        this.namespaces = namespaces;
    }

    public MetaCollection getMetaCollection(String database, String collection) throws DatabaseNotFoundException {
        if (!database.equals(supportedDatabase)) {
            throw new DatabaseNotFoundException(database);
        }
        if (NamespaceUtil.isNamespacesMetaCollection(collection)) {
            return namespaces;
        }
        if(NamespaceUtil.isIndexesMetaCollection(collection)) {
            return indexes;
        }
        LOGGER.warn("Requested an unsuported meta collection "+ collection + ". A mock is returned");
        return new NotSupportedMetaCollection(database, collection);
    }

}
