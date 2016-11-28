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

package com.torodb.mongodb.utils;

import com.eightkdata.mongowp.client.core.MongoClient;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.exceptions.NotMasterException;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.Service;
import com.torodb.core.exceptions.ToroRuntimeException;
import com.torodb.mongodb.commands.pojos.index.IndexOptions;
import com.torodb.mongodb.core.MongodServer;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

public interface DbCloner extends Service {

  /**
   *
   * @param dstDb
   * @param remoteClient
   * @param localServer
   * @param opts
   * @throws CloningException
   * @throws NotMasterException if {@link CloneOptions#getWritePermissionSupplier()
   *                            opts.getWritePermissionSupplier().get()} is evaluated to false
   */
  public void cloneDatabase(@Nonnull String dstDb, @Nonnull MongoClient remoteClient,
      MongodServer localServer, @Nonnull CloneOptions opts)
      throws CloningException, NotMasterException, MongoException;

  public static class CloneOptions {

    private final boolean cloneData;
    private final boolean cloneIndexes;
    private final boolean slaveOk;
    private final boolean snapshot;
    private final String dbToClone;
    private final Set<String> collsToIgnore;
    private final Supplier<Boolean> writePermissionSupplier;
    private final Predicate<String> collectionFilter;
    private final MyIndexPredicate indexFilter;

    public interface MyIndexPredicate {

      public boolean test(String collection, String indexName, boolean unique,
          List<IndexOptions.Key> keys);
    }

    public CloneOptions(
        boolean cloneData,
        boolean cloneIndexes,
        boolean slaveOk,
        boolean snapshot,
        String dbToClone,
        Set<String> collsToIgnore,
        Supplier<Boolean> writePermissionSupplier,
        Predicate<String> collectionFilter,
        MyIndexPredicate indexFilter) {
      this.cloneData = cloneData;
      this.cloneIndexes = cloneIndexes;
      this.slaveOk = slaveOk;
      this.snapshot = snapshot;
      this.dbToClone = dbToClone;
      this.collsToIgnore = collsToIgnore;
      this.writePermissionSupplier = writePermissionSupplier;
      this.collectionFilter = collectionFilter;
      this.indexFilter = indexFilter;
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
     * @return a supplier that can be used to know if write is allowed on the destiny database
     */
    public Supplier<Boolean> getWritePermissionSupplier() {
      return writePermissionSupplier;
    }

    /**
     * Returns a predicate that indicates which collections must be cloned.
     *
     * @return a predicate that is evaluated to true iff the collection with the given name must be
     *         cloned
     */
    public Predicate<String> getCollectionFilter() {
      return collectionFilter;
    }

    public MyIndexPredicate getIndexFilter() {
      return indexFilter;
    }
  }

  public static class CloningException extends ToroRuntimeException {

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
