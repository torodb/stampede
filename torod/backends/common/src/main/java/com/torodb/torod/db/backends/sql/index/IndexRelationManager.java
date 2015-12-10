package com.torodb.torod.db.backends.sql.index;

import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import com.torodb.torod.core.exceptions.ToroImplementationException;
import com.torodb.torod.core.pojos.NamedToroIndex;
import com.torodb.torod.core.pojos.UnnamedToroIndex;
import java.io.Closeable;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 *
 */
@ThreadSafe
public class IndexRelationManager implements Serializable {

    private static final long serialVersionUID = 1L;
    private volatile MemoryStructure commitedStructure;
    private final ReentrantLock writeLock;

    public IndexRelationManager() {
        commitedStructure = new MemoryStructure();
        writeLock = new ReentrantLock(true);
    }

    public IndexReadTransaction getReadTransaction() {
        return new IndexReadTransaction(getCommitedStructure());
    }
    
    public IndexWriteTransaction createWriteTransaction() {
        writeLock.lock();
        return new IndexWriteTransaction(this, writeLock);
    }
    
    private MemoryStructure getCommitedStructure() {
        return commitedStructure;
    }
    
    private void setCommitedStructure(MemoryStructure newStructure) {
        this.commitedStructure = newStructure;
    }

    private static class MemoryStructure implements Cloneable, Serializable {
        private static final long serialVersionUID = 1L;

        final Map<String, NamedToroIndex> toroNameToNamed;
        final HashBiMap<String, NamedDbIndex> dbNameToNamed;
        final HashBiMap<NamedToroIndex, UnnamedToroIndex> toroNamedToUnnamed;
        final HashBiMap<NamedDbIndex, UnnamedDbIndex> dbNamedToUnnamed;
        final HashMultimap<UnnamedToroIndex, UnnamedDbIndex> toroToDb;
        final HashMultimap<UnnamedDbIndex, UnnamedToroIndex> dbToToro;
        
        public MemoryStructure() {
            toroNameToNamed = Maps.newHashMap();
            dbNameToNamed = HashBiMap.create();

            toroNamedToUnnamed = HashBiMap.create();
            dbNamedToUnnamed = HashBiMap.create();

            toroToDb = HashMultimap.create();
            dbToToro = HashMultimap.create();
        }

        private MemoryStructure(MemoryStructure parent) {
            toroNameToNamed = Maps.newHashMap(parent.toroNameToNamed);
            dbNameToNamed = HashBiMap.create(parent.dbNameToNamed);

            toroNamedToUnnamed = HashBiMap.create(parent.toroNamedToUnnamed);
            dbNamedToUnnamed = HashBiMap.create(parent.dbNamedToUnnamed);

            toroToDb = HashMultimap.create(parent.toroToDb);
            dbToToro = HashMultimap.create(parent.dbToToro);
        }

        @Override
        protected MemoryStructure clone() {
            return new MemoryStructure(this);
        }

    }

    public static class IndexReadTransaction {

        private final MemoryStructure memory;

        IndexReadTransaction(MemoryStructure memory) {
            this.memory = memory;
        }
        
        MemoryStructure getMemory() {
            return memory;
        }

        public boolean existsToroIndex(String indexName) {
            return memory.toroNameToNamed.containsKey(indexName);
        }

        public boolean existsToroIndex(UnnamedToroIndex index) {
            return memory.toroToDb.containsKey(index);
        }

        public boolean existsDbIndex(String indexName) {
            return memory.dbNameToNamed.containsKey(indexName);
        }

        public boolean existsDbIndex(UnnamedDbIndex index) {
            return memory.dbToToro.containsKey(index);
        }

        public Set<UnnamedToroIndex> getToroUnnamedIndexes() {
            return memory.toroToDb.keySet();
        }

        public Set<NamedToroIndex> getToroNamedIndexes() {
            return memory.toroNamedToUnnamed.keySet();
        }

        public Set<UnnamedDbIndex> getDbIndexes() {
            return memory.dbToToro.keySet();
        }

        public Collection<UnnamedDbIndex> getRelatedDbIndexes(UnnamedToroIndex toroIndex) {
            Collection<UnnamedDbIndex> result = memory.toroToDb.get(toroIndex);
            return result;
        }

        public boolean exists(UnnamedToroIndex toroIndex) {
            return memory.toroToDb.containsKey(toroIndex);
        }

        public Collection<UnnamedToroIndex> getRelatedToroIndexes(UnnamedDbIndex dbIndex) {
            Collection<UnnamedToroIndex> result = memory.dbToToro.get(dbIndex);
            return result;
        }

        public boolean exists(UnnamedDbIndex dbIndex) {
            return memory.dbToToro.containsKey(dbIndex);
        }

        /**
         * Returns the existing {@link NamedToroIndex named toro index} that has
         * the same associated info than the candidate given or null if there is
         * no index that fulfil that condition
         * <p>
         * @param candidate
         * @return
         */
        @Nullable
        public NamedToroIndex getToroIndex(UnnamedToroIndex candidate) {
            return memory.toroNamedToUnnamed.inverse().get(candidate);
        }
        
        /**
         * Returns the existing {@link NamedDbIndex named db index} that has the
         * same associated info than the candidate given or null if there is no
         * index that fulfil that condition
         * <p>
         * @param candidate
         * @return
         */
        @Nullable
        public NamedDbIndex getDbIndex(UnnamedDbIndex candidate) {
            return memory.dbNamedToUnnamed.inverse().get(candidate);
        }

        public NamedToroIndex getToroIndex(String name) {
            NamedToroIndex result = memory.toroNameToNamed.get(name);
            if (result == null) {
                throw new IllegalArgumentException("There is no toro index called '"
                        + name + '\'');
            }
            return result;
        }

        public NamedDbIndex getDbIndex(String name) {
            NamedDbIndex result = memory.dbNameToNamed.get(name);
            if (result == null) {
                throw new IllegalArgumentException("There is no db index called '"
                        + name + '\'');
            }
            return result;
        }

    }

    public static class IndexWriteTransaction extends IndexReadTransaction implements Closeable {

        private final IndexRelationManager manager;
        private final ReentrantLock lock;

        public IndexWriteTransaction(IndexRelationManager manager, ReentrantLock writeLock) {
            super(manager.getCommitedStructure().clone());
            this.manager = manager;
            this.lock = writeLock;
        }

        public void commit() {
            manager.setCommitedStructure(getMemory());
        }

        @Override
        public void close() {
            this.lock.unlock();
        }

        public void storeToroIndex(NamedToroIndex namedToroIndex) {
            MemoryStructure memory = getMemory();
            Preconditions.checkArgument(
                    !memory.toroNameToNamed.containsKey(namedToroIndex.getName())
            );
            Preconditions.checkArgument(
                    !memory.toroNamedToUnnamed.containsKey(namedToroIndex)
            );

            memory.toroNameToNamed.put(namedToroIndex.getName(), namedToroIndex);
            memory.toroNamedToUnnamed.put(namedToroIndex, namedToroIndex.asUnnamed());
        }

        public void storeDbIndex(NamedDbIndex namedDbIndex) {
            MemoryStructure memory = getMemory();
            
            Preconditions.checkArgument(
                    !memory.dbNameToNamed.containsKey(namedDbIndex.getName())
            );
            Preconditions.checkArgument(
                    !memory.dbNamedToUnnamed.containsKey(namedDbIndex)
            );

            memory.dbNameToNamed.put(namedDbIndex.getName(), namedDbIndex);
            memory.dbNamedToUnnamed.put(namedDbIndex, namedDbIndex.asUnnamed());
        }

        public void addIndexRelation(String toroIndexName, String dbIndexName) {
            MemoryStructure memory = getMemory();
            NamedToroIndex namedToroIndex = memory.toroNameToNamed.get(toroIndexName);
            if (namedToroIndex == null) {
                throw new IllegalArgumentException(
                        "There is no toro index called " + toroIndexName
                );
            }
            NamedDbIndex namedDbIndex = memory.dbNameToNamed.get(dbIndexName);
            if (namedDbIndex == null) {
                throw new IllegalArgumentException(
                        "There is no db index called " + dbIndexName
                );
            }

            UnnamedToroIndex unnamedToroIndex
                    = memory.toroNamedToUnnamed.get(namedToroIndex);
            assert unnamedToroIndex != null;
            UnnamedDbIndex unnamedDbIndex = memory.dbNamedToUnnamed.get(namedDbIndex);
            assert unnamedDbIndex != null;

            if (memory.toroToDb.containsEntry(unnamedToroIndex, unnamedDbIndex)) {
                assert memory.dbToToro.containsEntry(unnamedDbIndex, unnamedToroIndex);
                return ;
            }

            boolean modified = memory.toroToDb.put(unnamedToroIndex, unnamedDbIndex);
            assert modified;
            
            modified = memory.dbToToro.put(unnamedDbIndex, unnamedToroIndex);
            assert modified;
        }

        public Set<NamedDbIndex> removeToroIndex(String toroIndexName) {
            MemoryStructure memory = getMemory();
            NamedToroIndex toroNamed = memory.toroNameToNamed.remove(toroIndexName);
            if (toroNamed == null) {
                throw new IllegalArgumentException("There is no toro index called "
                        + toroIndexName);
            }

            UnnamedToroIndex toroUnnamed = memory.toroNamedToUnnamed.remove(toroNamed);
            if (toroUnnamed == null) {
                throw new ToroImplementationException("A relation between "
                        + toroNamed + " and a unnamed toro index was expected"
                );
            }

            Collection<UnnamedDbIndex> dbUnnameds
                    = memory.toroToDb.removeAll(toroUnnamed);
            if (dbUnnameds.isEmpty()) { //this can happen when there is no structure that matches the index
                return Collections.emptySet();
            }

            Set<NamedDbIndex> result
                    = Sets.newHashSetWithExpectedSize(dbUnnameds.size());

            for (UnnamedDbIndex dbUnnamed : dbUnnameds) {

                boolean removed = memory.dbToToro.remove(dbUnnamed, toroUnnamed);
                assert removed;

                if (!memory.dbToToro.containsKey(dbUnnamed)) {
                    NamedDbIndex dbNamed
                            = memory.dbNamedToUnnamed.inverse().remove(dbUnnamed);
                    if (dbNamed == null) {
                        throw new ToroImplementationException("It was expected that "
                                + "the existent unnamed db index " + dbUnnamed
                                + " was "
                                + "associated with named db index, but it is not"
                        );
                    }
                    result.add(dbNamed);
                    if (memory.dbNameToNamed.inverse().remove(dbNamed) == null) {
                        throw new ToroImplementationException("It was expected that "
                                + "the existent named db index " + dbNamed
                                + " was "
                                + "associated with a name, but it is not"
                        );
                    }
                }
            }

            return result;
        }
    }
}
