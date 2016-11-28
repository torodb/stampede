/*
 * ToroDB
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.mongodb.utils.cloner;

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.WriteConcern;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.client.core.MongoClient;
import com.eightkdata.mongowp.client.core.MongoConnection;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.exceptions.NotMasterException;
import com.eightkdata.mongowp.messages.request.QueryMessage.QueryOption;
import com.eightkdata.mongowp.messages.request.QueryMessage.QueryOptions;
import com.eightkdata.mongowp.server.api.Request;
import com.eightkdata.mongowp.server.api.impl.CollectionCommandArgument;
import com.eightkdata.mongowp.server.api.pojos.MongoCursor;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.AbstractService;
import com.torodb.mongodb.commands.pojos.CollectionOptions;
import com.torodb.mongodb.commands.pojos.CursorResult;
import com.torodb.mongodb.commands.pojos.index.IndexOptions;
import com.torodb.mongodb.commands.signatures.admin.CreateCollectionCommand;
import com.torodb.mongodb.commands.signatures.admin.CreateCollectionCommand.CreateCollectionArgument;
import com.torodb.mongodb.commands.signatures.admin.CreateIndexesCommand;
import com.torodb.mongodb.commands.signatures.admin.CreateIndexesCommand.CreateIndexesArgument;
import com.torodb.mongodb.commands.signatures.admin.DropCollectionCommand;
import com.torodb.mongodb.commands.signatures.admin.ListCollectionsCommand.ListCollectionsResult.Entry;
import com.torodb.mongodb.commands.signatures.general.InsertCommand;
import com.torodb.mongodb.commands.signatures.general.InsertCommand.InsertArgument;
import com.torodb.mongodb.commands.signatures.general.InsertCommand.InsertResult;
import com.torodb.mongodb.core.MongodConnection;
import com.torodb.mongodb.core.MongodServer;
import com.torodb.mongodb.core.WriteMongodTransaction;
import com.torodb.mongodb.utils.DbCloner;
import com.torodb.mongodb.utils.ListCollectionsRequester;
import com.torodb.mongodb.utils.ListIndexesRequester;
import com.torodb.mongodb.utils.NamespaceUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.inject.Singleton;

/**
 *
 */
@Singleton
public class TransactionalDbCloner extends AbstractService implements DbCloner {

  private static final Logger LOGGER = LogManager.getLogger(DbCloner.class);

  @Override
  protected void doStart() {
    notifyStarted();
  }

  @Override
  protected void doStop() {
    notifyStopped();
  }

  @Override
  public void cloneDatabase(String dstDb, MongoClient remoteClient,
      MongodServer localServer, CloneOptions opts) throws CloningException,
      NotMasterException, MongoException {

    try (MongoConnection remoteConnection = remoteClient.openConnection();
        MongodConnection localConnection = localServer.openConnection();
        WriteMongodTransaction transaction = localConnection.openWriteTransaction(true)) {
      cloneDatabase(dstDb, remoteConnection, transaction, opts);
    }
  }

  /**
   *
   * @param dstDb
   * @param remoteConnection
   * @param transaction
   * @param opts
   * @throws CloningException
   * @throws NotMasterException if {@link CloneOptions#getWritePermissionSupplier()
   *                            opts.getWritePermissionSupplier().get()} is evaluated to false
   */
  public void cloneDatabase(
      @Nonnull String dstDb,
      @Nonnull MongoConnection remoteConnection,
      @Nonnull WriteMongodTransaction transaction,
      @Nonnull CloneOptions opts
  ) throws CloningException, NotMasterException, MongoException {
    if (!remoteConnection.isRemote() && opts.getDbToClone().equals(dstDb)) {
      LOGGER.warn("Trying to clone a database to itself! Ignoring it");
      return;
    }
    String fromDb = opts.getDbToClone();

    CursorResult<Entry> listCollections;
    try {
      listCollections = ListCollectionsRequester.getListCollections(
          remoteConnection,
          fromDb,
          null
      );
    } catch (MongoException ex) {
      throw new CloningException(
          "It was impossible to get information from the remote server",
          ex
      );
    }

    if (!opts.getWritePermissionSupplier().get()) {
      throw new NotMasterException("Destiny database cannot be written");
    }

    Map<String, CollectionOptions> collsToClone = Maps.newHashMap();
    for (Iterator<Entry> iterator = listCollections.getFirstBatch(); iterator.hasNext();) {
      Entry collEntry = iterator.next();
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
      LOGGER.info("Collection {}.{} will be cloned", fromDb, collName);
      collsToClone.put(collName, collEntry.getCollectionOptions());
    }

    if (!opts.getWritePermissionSupplier().get()) {
      throw new NotMasterException("Destiny database cannot be written "
          + "after get collections info");
    }

    for (Map.Entry<String, CollectionOptions> entry : collsToClone.entrySet()) {
      dropCollection(transaction, dstDb, entry.getKey());
      createCollection(transaction, dstDb, entry.getKey(), entry.getValue());
    }
    if (opts.isCloneData()) {
      for (Map.Entry<String, CollectionOptions> entry : collsToClone.entrySet()) {
        cloneCollection(dstDb, remoteConnection, transaction, opts, entry.getKey(),
            entry.getValue());
      }
    }
    if (opts.isCloneIndexes()) {
      for (Map.Entry<String, CollectionOptions> entry : collsToClone.entrySet()) {
        cloneIndex(dstDb, remoteConnection, transaction, opts, entry.getKey(), entry.getValue());
      }
    }
  }

  private void cloneCollection(
      String toDb,
      MongoConnection remoteConnection,
      WriteMongodTransaction transaction,
      CloneOptions opts,
      String collection,
      CollectionOptions collOptions) throws MongoException, CloningException {
    String fromDb = opts.getDbToClone();
    LOGGER.info("Cloning {}.{} into {}.{}", fromDb, collection, toDb, collection);

    //TODO: enable exhaust?
    EnumSet<QueryOption> queryFlags = EnumSet.of(QueryOption.NO_CURSOR_TIMEOUT);
    if (opts.isSlaveOk()) {
      queryFlags.add(QueryOption.SLAVE_OK);
    }
    MongoCursor<BsonDocument> cursor = remoteConnection.query(
        opts.getDbToClone(),
        collection,
        null,
        0,
        0,
        new QueryOptions(queryFlags),
        null,
        null
    );
    while (!cursor.hasNext()) {
      List<? extends BsonDocument> docsToInsert = cursor.fetchBatch().asList();

      Status<InsertResult> insertResult = transaction.execute(
          new Request(toDb, null, true, null),
          InsertCommand.INSTANCE,
          new InsertArgument.Builder(collection)
              .addDocuments(docsToInsert)
              .setWriteConcern(WriteConcern.fsync())
              .setOrdered(true)
              .build()
      );
      if (!insertResult.isOk() || insertResult.getResult().getN() != docsToInsert.size()) {
        throw new CloningException("Error while inserting a cloned document");
      }
    }
  }

  private void cloneIndex(
      String dstDb,
      MongoConnection remoteConnection,
      WriteMongodTransaction transaction,
      CloneOptions opts,
      String fromCol,
      CollectionOptions collOptions) throws CloningException {
    try {
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

      Status<?> status;

      List<IndexOptions> indexes = Lists.newArrayList(
          ListIndexesRequester.getListCollections(remoteConnection, dstDb, fromCol).getFirstBatch()
      );
      if (indexes.isEmpty()) {
        return;
      }

      status = transaction.execute(
          new Request(dstDb, null, true, null),
          CreateIndexesCommand.INSTANCE,
          new CreateIndexesArgument(
              fromCol,
              indexes
          )
      );
      if (!status.isOk()) {
        throw new CloningException("Error while trying to fetch indexes from remote: " + status);
      }
    } catch (MongoException ex) {
      throw new CloningException("Error while trying to fetch indexes from remote", ex);
    }
  }

  private Status<?> createCollection(
      WriteMongodTransaction transaction,
      String db,
      String collection,
      CollectionOptions options) {
    return transaction.execute(
        new Request(db, null, true, null),
        CreateCollectionCommand.INSTANCE,
        new CreateCollectionArgument(collection, options)
    );
  }

  private Status<?> dropCollection(
      WriteMongodTransaction transaction,
      String db,
      String collection) {
    return transaction.execute(
        new Request(db, null, true, null),
        DropCollectionCommand.INSTANCE,
        new CollectionCommandArgument(collection, DropCollectionCommand.INSTANCE)
    );
  }
}
