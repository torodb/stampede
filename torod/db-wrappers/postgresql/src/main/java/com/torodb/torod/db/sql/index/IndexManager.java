package com.torodb.torod.db.sql.index;

import com.google.common.collect.*;
import com.torodb.torod.core.exceptions.ToroImplementationException;
import com.torodb.torod.core.pojos.NamedToroIndex;
import com.torodb.torod.core.pojos.UnnamedToroIndex;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 *
 */
@NotThreadSafe
public class IndexManager {

    private final Map<String, NamedToroIndex> toroNameToNamed;
    private final BiMap<String, NamedDbIndex> dbNameToNamed;
    private final BiMap<NamedToroIndex, UnnamedToroIndex> toroNamedToUnnamed;
    private final BiMap<NamedDbIndex, UnnamedDbIndex> dbNamedToUnnamed;
    private final Multimap<UnnamedToroIndex, UnnamedDbIndex> toroToDb;
    private final Multimap<UnnamedDbIndex, UnnamedToroIndex> dbToToro;
    private final ReentrantReadWriteLock lock;

    public IndexManager() {
        toroNameToNamed = Maps.newHashMap();
        dbNameToNamed = HashBiMap.create();

        toroNamedToUnnamed = HashBiMap.create();
        dbNamedToUnnamed = HashBiMap.create();

        toroToDb = HashMultimap.create();
        dbToToro = HashMultimap.create();

        lock = new ReentrantReadWriteLock(true);
    }

    public Lock getWriteLock() {
        return lock.writeLock();
    }

    public Lock getReadLock() {
        return lock.readLock();
    }

    public boolean existsToroIndex(String indexName) {
        return toroNameToNamed.containsKey(indexName);
    }

    public boolean existsToroIndex(UnnamedToroIndex index) {
        return toroToDb.containsKey(index);
    }

    public boolean existsDbIndex(String indexName) {
        return dbNameToNamed.containsKey(indexName);
    }

    public boolean existsDbIndex(UnnamedDbIndex index) {
        return dbToToro.containsKey(index);
    }

    public Set<UnnamedToroIndex> getToroUnnamedIndexes() {
        return toroToDb.keySet();
    }

    public Set<NamedToroIndex> getToroNamedIndexes() {
        return toroNamedToUnnamed.keySet();
    }

    public Set<UnnamedDbIndex> getDbIndexes() {
        return dbToToro.keySet();
    }

    public Collection<UnnamedDbIndex> getDbIndexes(UnnamedToroIndex toroIndex) {
        Collection<UnnamedDbIndex> result = toroToDb.get(toroIndex);
        if (result.isEmpty()) {
            throw new IllegalArgumentException("Torodb index '" + toroIndex
                    + "' is not stored in this index relation");
        }
        return result;
    }

    public Collection<UnnamedToroIndex> getToroIndexes(UnnamedDbIndex dbIndex) {
        Collection<UnnamedToroIndex> result = dbToToro.get(dbIndex);
        if (result.isEmpty()) {
            throw new IllegalArgumentException("DB index '" + dbIndex
                    + "' is not stored in this index relation");
        }
        return result;
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
        return dbNamedToUnnamed.inverse().get(candidate);
    }

    private void checkValidToInsert(NamedToroIndex toroIndex) {
        if (existsToroIndex(toroIndex.getName())) {
            throw new IllegalArgumentException(
                    "ToroIndex with name " + toroIndex.getName()
                    + " already "
                    + "exists"
            );
        }
        if (existsToroIndex(toroIndex.asUnnamed())) {
            throw new IllegalArgumentException(
                    "ToroIndex like " + toroIndex.asUnnamed()
                    + " already exists"
            );
        }
    }

    private void checkValidToInsert(NamedDbIndex named) {
        NamedDbIndex oldNamed = dbNameToNamed.get(named.getName());
        if (oldNamed == null) {
            return ;
        }
        if (!oldNamed.equals(named)) {
            throw new IllegalArgumentException(
                    "DbIndex like " + named.asUnnamed() + " already exists");
        }
    }

    public void addIndexRelation(NamedToroIndex toroNamed, NamedDbIndex dbNamed) {
        checkValidToInsert(dbNamed);
        checkValidToInsert(toroNamed);

        UnnamedToroIndex toroUnnamed = toroNamed.asUnnamed();
        UnnamedDbIndex dbUnnamed = dbNamed.asUnnamed();

        boolean modified;

        //connect name with toro named index
        modified = toroNameToNamed.putIfAbsent(toroNamed.getName(), toroNamed) != null;
        assert modified;

        //connect toro named index with unnamed index
        modified = toroNamedToUnnamed.putIfAbsent(toroNamed, toroUnnamed) != null;
        assert modified;

        //connect toro unnamed with db unnamed
        modified = toroToDb.put(toroUnnamed, dbUnnamed);
        assert modified : "There is a previous relation from '"
                + toroUnnamed + "' to '" + dbUnnamed + "'";
        
        //connect db unnamed with db named
        UnnamedDbIndex oldUnamed = dbNamedToUnnamed.putIfAbsent(dbNamed, dbUnnamed);
        assert oldUnamed == null || oldUnamed.equals(dbUnnamed);

        //connect db named with its name
        NamedDbIndex oldNamed = dbNameToNamed.putIfAbsent(dbNamed.getName(), dbNamed);
        assert oldNamed == null || oldNamed.equals(dbNamed);
    }

    public Set<NamedDbIndex> removeToroIndex(String toroIndexName) {

        NamedToroIndex toroNamed = toroNameToNamed.remove(toroIndexName);
        if (toroNamed == null) {
            throw new IllegalArgumentException("There is no toro index called "
                    + toroIndexName);
        }

        UnnamedToroIndex toroUnnamed = toroNamedToUnnamed.remove(toroNamed);
        if (toroUnnamed == null) {
            throw new ToroImplementationException("A relation between "
                    + toroNamed + " and a unnamed toro index was expected"
            );
        }
        
        Collection<UnnamedDbIndex> dbUnnameds = toroToDb.removeAll(toroUnnamed);
        if (dbUnnameds.isEmpty()) {
            throw new ToroImplementationException("A relation between "
                    + toroIndexName + " and at least one unnamed db index was "
                    + "expected"
            );
        }
        
        Set<NamedDbIndex> result = Sets.newHashSetWithExpectedSize(dbUnnameds.size());
        
        for (UnnamedDbIndex dbUnnamed : dbUnnameds) {

            boolean removed = dbToToro.remove(dbUnnamed, toroUnnamed);
            assert removed;
            
            if (!dbToToro.containsKey(dbUnnamed)) {
                NamedDbIndex dbNamed = dbNamedToUnnamed.inverse().remove(dbUnnamed);
                if (dbNamed == null) {
                    throw new ToroImplementationException("It was expected that "
                            + "the existent unnamed db index " + dbUnnamed + " was "
                            + "associated with named db index, but it is not"
                    );
                }
                result.add(dbNamed);
                if (dbNameToNamed.inverse().remove(dbNamed) == null) {
                    throw new ToroImplementationException("It was expected that "
                            + "the existent named db index " + dbNamed + " was "
                            + "associated with a name, but it is not"

                    );
                }
            }
        }
        
        return result;
    }
}
