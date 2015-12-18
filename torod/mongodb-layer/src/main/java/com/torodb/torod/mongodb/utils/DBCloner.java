package com.torodb.torod.mongodb.utils;

import com.eightkdata.mongowp.client.core.MongoClient;
import com.eightkdata.mongowp.client.core.MongoConnection;
import com.eightkdata.mongowp.messages.request.QueryMessage;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.CollectionCommandArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.CreateCollectionCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.CreateCollectionCommand.CreateCollectionArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.CreateIndexesCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.CreateIndexesCommand.CreateIndexesArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.DropCollectionCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.ListCollectionsCommand.ListCollectionsResult.Entry;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.InsertCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.InsertCommand.InsertArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.InsertCommand.InsertResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.CollectionOptions;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.IndexOptions;
import com.eightkdata.mongowp.mongoserver.api.safe.pojos.MongoCursor;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.MongoException;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.NotMasterException;
import com.google.common.annotations.Beta;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import com.mongodb.MongoClientException;
import com.mongodb.WriteConcern;
import com.torodb.torod.core.annotations.DatabaseName;
import com.torodb.torod.mongodb.impl.LocalMongoConnection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
/*
 * TODO: This class must be improved. Cloning is not transactional and it has
 * other problems or things that must be improved
 */
@Singleton
@Beta
public class DBCloner {

    private static final Logger LOGGER = LoggerFactory.getLogger(DBCloner.class);
    private final String supportedDatabase;

    @Inject
    public DBCloner(@DatabaseName String supportedDatabase) {
        this.supportedDatabase = supportedDatabase;
    }

    public void cloneDatabase(
            @Nonnull String dstDb,
            @Nonnull MongoClient remoteClient,
            @Nonnull LocalMongoConnection localConnection,
            @Nonnull CloneOptions opts) throws CloningException, NotMasterException, MongoException {
        MongoConnection remoteConnection = remoteClient.openConnection();
        try {
            cloneDatabase(dstDb, remoteConnection, localConnection, opts);
        } finally {
            remoteConnection.close();
        }
    }

    /**
     *
     * @param dstDb
     * @param remoteConnection
     * @param localConnection
     * @param opts
     * @throws CloningException
     * @throws NotMasterException if {@link CloneOptions#getWritePermissionSupplier()
     *                            opts.getWritePermissionSupplier().get()} is
     *                            evaluated to false
     */
    public void cloneDatabase(
            @Nonnull String dstDb,
            @Nonnull MongoConnection remoteConnection,
            @Nonnull LocalMongoConnection localConnection,
            @Nonnull CloneOptions opts
    ) throws CloningException, NotMasterException, MongoException {
        if (!supportedDatabase.equals(dstDb)) {
            LOGGER.warn("Db {} will not be cloned because it is an usupported database", dstDb);
            return ;
        }
        if (!remoteConnection.isRemote() && opts.getDbToClone().equals(dstDb)) {
            LOGGER.warn("Trying to clone a database to itself! Ignoring it");
            return;
        }
        String fromDb = opts.getDbToClone();

        MongoCursor<Entry> listCollections;
        try {
            listCollections = ListCollectionsRequester.getListCollections(
                    remoteConnection,
                    fromDb,
                    null
            );
        } catch (MongoClientException ex) {
            throw new CloningException(
                    "It was impossible to get information from the remote server",
                    ex
            );
        }

        if (!opts.getWritePermissionSupplier().get()) {
            throw new NotMasterException("Destiny database cannot be written");
        }

        Map<String, CollectionOptions> collsToClone = Maps.newHashMap();
        for (Entry collEntry : listCollections) {
            String collName = collEntry.getCollectionName();

            if (opts.getCollsToIgnore().contains(collName)) {
                LOGGER.debug("Not cloning {} because is marked as an ignored collection", collName);
                continue;
            }

            if (!NamespaceUtil.isUserWritable(fromDb, collName)) {
                LOGGER.info("Not cloning {} because is a not user writable", collName);
                continue;
            }
            if (NamespaceUtil.isNormal(fromDb, collName)) {
                LOGGER.info("Not cloning {} because it is not normal", collName);
                continue;
            }
            LOGGER.info("Collection {} will be cloned", collName);
            collsToClone.put(collName, collEntry.getCollectionOptions());
        }

        if (!opts.getWritePermissionSupplier().get()) {
            throw new NotMasterException("Destiny database cannot be written "
                    + "after get collections info");
        }

        for (Map.Entry<String, CollectionOptions> entry : collsToClone.entrySet()) {
            dropCollection(localConnection, dstDb, entry.getKey());
            createCollection(localConnection, dstDb, entry.getKey(), entry.getValue());
        }
        if (opts.isCloneData()) {
            for (Map.Entry<String, CollectionOptions> entry : collsToClone.entrySet()) {
                cloneCollection(dstDb, remoteConnection, localConnection, opts, entry.getKey(), entry.getValue());
            }
        }
        if (opts.isCloneIndexes()) {
            for (Map.Entry<String, CollectionOptions> entry : collsToClone.entrySet()) {
                cloneIndex(dstDb, remoteConnection, localConnection, opts, entry.getKey(), entry.getValue());
            }
        }
    }

    private void cloneCollection(
            String toDb,
            MongoConnection remoteConnection,
            @Nonnull LocalMongoConnection localConnection,
            CloneOptions opts,
            String collection,
            CollectionOptions collOptions) throws MongoException, CloningException {
        String fromDb = opts.getDbToClone();
        LOGGER.info("Clonning {}.{} into {}.{}", fromDb, collection, toDb, collection);

        EnumSet<QueryMessage.Flag> queryFlags = EnumSet.of(QueryMessage.Flag.NO_CURSOR_TIMEOUT); //TODO: enable exhaust?
        if (opts.slaveOk) {
            queryFlags.add(QueryMessage.Flag.SLAVE_OK);
        }
        MongoCursor<BsonDocument> cursor = remoteConnection.query(
                opts.getDbToClone(),
                collection,
                queryFlags,
                null,
                0,
                0,
                null
        );
        while (!cursor.isDead()) {
            List<? extends BsonDocument> docsToInsert = cursor.fetchBatch().asList();

            InsertResult insertResult = localConnection.execute(
                    InsertCommand.INSTANCE,
                    toDb,
                    true,
                    new InsertArgument.Builder(collection)
                        .addDocuments(docsToInsert)
                        .setWriteConcern(WriteConcern.FSYNCED)
                        .setOrdered(true)
                        .build()
            );
            if (insertResult.getN() != docsToInsert.size()) {
                throw new CloningException("Error while inserting a cloned document");
            }
        }
    }

    private void cloneIndex(
            String dstDb,
            MongoConnection remoteConnection,
            @Nonnull LocalMongoConnection localConnection,
            CloneOptions opts,
            String fromCol,
            CollectionOptions collOptions) throws CloningException {
        String fromDb = opts.getDbToClone();
        HostAndPort remoteAddress = remoteConnection.getClientOwner().getAddress();
        String remoteAddressString = remoteAddress != null ? remoteAddress.toString() : "local";
        LOGGER.info("copying indexes from {}.{} on {} to {}.{} on local server", 
                fromDb, 
                fromCol, 
                remoteAddressString,
                dstDb, 
                fromCol
        );

        try {
            List<IndexOptions> indexes = Lists.newArrayList(
                    ListIndexesRequester.getListCollections(remoteConnection, dstDb, fromCol)
                            .iterator()
            );
            if (indexes.isEmpty()) {
                return ;
            }

            List<IndexOptions> indexesToCreate = Lists.newArrayListWithCapacity(indexes.size());
            for (IndexOptions index : indexes) {
                indexesToCreate.add(index);
            }
            localConnection.execute(
                    CreateIndexesCommand.INSTANCE,
                    dstDb,
                    true,
                    new CreateIndexesArgument(
                            fromCol,
                            indexesToCreate
                    )
            );
        } catch (MongoException ex) {
            throw new CloningException("Error while trying to fetch indexes from remote", ex);
        }
    }

    private void createCollection(
            LocalMongoConnection localConnection,
            String db,
            String collection,
            CollectionOptions options) throws MongoException, CloningException {
        localConnection.execute(
                CreateCollectionCommand.INSTANCE,
                db,
                true,
                new CreateCollectionArgument(collection, options)
        );
    }

    private void dropCollection(
            LocalMongoConnection localConnection,
            String db,
            String collection) throws MongoException, CloningException {
        localConnection.execute(
                DropCollectionCommand.INSTANCE,
                db,
                true,
                new CollectionCommandArgument(collection, DropCollectionCommand.INSTANCE)
        );
    }

    public static class CloneOptions {

        private final boolean cloneData;
        private final boolean cloneIndexes;
        private final boolean slaveOk;
        private final boolean snapshot;
        private final String dbToClone;
        private final Set<String> collsToIgnore;
        private final Supplier<Boolean> writePermissionSupplier;

        public CloneOptions(
                boolean cloneData,
                boolean cloneIndexes,
                boolean slaveOk,
                boolean snapshot,
                String dbToClone,
                Set<String> collsToIgnore,
                Supplier<Boolean> writePermissionSupplier) {
            this.cloneData = cloneData;
            this.cloneIndexes = cloneIndexes;
            this.slaveOk = slaveOk;
            this.snapshot = snapshot;
            this.dbToClone = dbToClone;
            this.collsToIgnore = collsToIgnore;
            this.writePermissionSupplier = writePermissionSupplier;
        }

        /**
         * @return true iff data must be cloned
         */
        public boolean isCloneData() {
            return cloneData;
        }

        /**
         * @return true iff indexes must be cloned
         */
        public boolean isCloneIndexes() {
            return cloneIndexes;
        }

        /**
         * @return true iff is ok to clone from a node that is not master
         */
        public boolean isSlaveOk() {
            return slaveOk;
        }

        /**
         * @return true iff $snapshot must be used
         */
        public boolean isSnapshot() {
            return snapshot;
        }

        /**
         * @return the database that will be cloned
         */
        public String getDbToClone() {
            return dbToClone;
        }

        /**
         * @return a set of collections that will not be cloned
         */
        @Nonnull
        public Set<String> getCollsToIgnore() {
            return collsToIgnore;
        }

        /**
         * @return a supplier that can be used to know if write is allowed on
         *         the destiny database
         */
        public Supplier<Boolean> getWritePermissionSupplier() {
            return writePermissionSupplier;
        }

    }

    public static class CloningException extends Exception {
        private static final long serialVersionUID = 1L;

        public CloningException() {
        }

        public CloningException(String message) {
            super(message);
        }

        public CloningException(String message, Throwable cause) {
            super(message, cause);
        }

        public CloningException(Throwable cause) {
            super(cause);
        }
        
    }
}
