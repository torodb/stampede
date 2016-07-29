package com.torodb.mongodb.utils;

import com.eightkdata.mongowp.client.core.MongoClient;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.exceptions.NotMasterException;
import com.google.common.base.Supplier;
import com.torodb.core.exceptions.ToroRuntimeException;
import com.torodb.mongodb.core.MongodServer;
import java.util.Set;
import javax.annotation.Nonnull;

public interface DbCloner {
    /**
     *
     * @throws CloningException
     * @throws NotMasterException if {@link CloneOptions#getWritePermissionSupplier()
     *                            opts.getWritePermissionSupplier().get()} is
     *                            evaluated to false
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
