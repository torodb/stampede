
package com.torodb.torod.mongodb.crp;

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
public class CollectionRequestProcessorProvider {

    private static final Logger LOGGER
            = LoggerFactory.getLogger(CollectionRequestProcessorProvider.class);

    private final String supportedDatabase;
    private final MetaCollectionRequestProcessor indexCollection;
    private final MetaCollectionRequestProcessor namespacesCollection;
    private final StandardCollectionRequestProcessor standardCollection;

    @Inject
    public CollectionRequestProcessorProvider(
            @DatabaseName String supportedDatabase,
            @Index MetaCollectionRequestProcessor indexCollection,
            @Namespaces MetaCollectionRequestProcessor namespacesCollection,
            StandardCollectionRequestProcessor standardCollection) {
        this.supportedDatabase = supportedDatabase;
        this.indexCollection = indexCollection;
        this.namespacesCollection = namespacesCollection;
        this.standardCollection = standardCollection;
    }

    public CollectionRequestProcessor getCollectionRequestProcessor(String database, String collection) {
        if (!database.equals(supportedDatabase)) {
            LOGGER.warn("Requested the unsupported database " + database + ". "
                    + "A mock collection is returned");
            return new NotSupportedDatabaseCollection(database);
        }
        if (NamespaceUtil.isNamespacesMetaCollection(collection)) {
            return namespacesCollection;
        }
        if(NamespaceUtil.isIndexesMetaCollection(collection)) {
            return indexCollection;
        }
        return standardCollection;
    }


}
