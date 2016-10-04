/*
 * This file is part of ToroDB.
 *
 * ToroDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ToroDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with core. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.metainfo.cache.mvcc;

import java.util.Optional;

import org.jooq.lambda.Seq;
import org.jooq.lambda.tuple.Tuple2;

import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPartIndex;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPartIndexColumn;
import com.torodb.core.transaction.metainf.ImmutableMetaField;
import com.torodb.core.transaction.metainf.ImmutableMetaIndex;
import com.torodb.core.transaction.metainf.ImmutableMetaIndexField;
import com.torodb.core.transaction.metainf.ImmutableMetaScalar;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaDocPartIndex;
import com.torodb.core.transaction.metainf.MetaElementState;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaIndex;
import com.torodb.core.transaction.metainf.MetaScalar;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import com.torodb.core.transaction.metainf.MutableMetaDocPartIndex;
import com.torodb.core.transaction.metainf.MutableMetaIndex;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import com.torodb.core.transaction.metainf.UnmergeableException;

/**
 *
 */
public class SnapshotMerger {

    private final ImmutableMetaSnapshot oldSnapshot;
    private final MutableMetaSnapshot newSnapshot;

    public SnapshotMerger(ImmutableMetaSnapshot oldSnapshot, MutableMetaSnapshot newSnapshot) {
        this.oldSnapshot = oldSnapshot;
        this.newSnapshot = newSnapshot;
    }

    public ImmutableMetaSnapshot.Builder merge() throws UnmergeableException {
        ImmutableMetaSnapshot.Builder builder = new ImmutableMetaSnapshot.Builder(oldSnapshot);
        Iterable<Tuple2<MutableMetaDatabase, MetaElementState>> changes = newSnapshot.getModifiedDatabases();
        for (Tuple2<MutableMetaDatabase, MetaElementState> change : changes) {
            merge(builder, change.v1(), change.v2());
        }
        return builder;
    }

    private void merge(ImmutableMetaSnapshot.Builder parentBuilder, MutableMetaDatabase newDb,
            MetaElementState newState) throws UnmergeableException {

        ImmutableMetaDatabase byName = oldSnapshot.getMetaDatabaseByName(newDb.getName());
        ImmutableMetaDatabase byId = oldSnapshot.getMetaDatabaseByIdentifier(newDb.getIdentifier());

        switch (newState) {
            case NOT_CHANGED:
            case NOT_EXISTENT:
                throw new AssertionError("A modification was expected, but the new state is " + newState);
            case ADDED:
            case MODIFIED: {
                if (byName != byId) {
                    throw createUnmergeableException(newDb, byName, byId);
                }
                if (byName == null && byId == null) {
                    parentBuilder.put(newDb.immutableCopy());
                    return ;
                }
                assert byName != null;
                assert byId != null;

                ImmutableMetaDatabase.Builder childBuilder = new ImmutableMetaDatabase.Builder(byId);
                for (Tuple2<MutableMetaCollection, MetaElementState> modifiedCollection : newDb.getModifiedCollections()) {
                    merge(newDb, byId, childBuilder, modifiedCollection.v1(), modifiedCollection.v2());
                }
                parentBuilder.put(childBuilder);
                break;
            }
            case REMOVED: {
                if (byName != byId) {
                    /*
                     * The backend transaction will remove by id, but it is referencing another name
                     * on the current snapshot, so the final state will be inconsistent.
                     * It is better to fail.
                     */
                    throw createUnmergeableException(newDb, byName, byId);
                }
                if (byName == null && byId == null) { 
                    /*
                     * it has been removed on another transaction or created and removed on the 
                     * current one. No change must be done
                     */
                    return ;
                }
                assert byName != null;
                assert byId != null;
                /*
                 * In this case, we can delegate on the backend transaction check. If it thinks
                 * everything is fine, we can remove the element. If it thinks there is an error,
                 * then we have to rollback the transaction.
                 */
                parentBuilder.remove(byName);
            }
        }

    }

    private void merge(MetaDatabase newStructure, ImmutableMetaDatabase oldStructure, ImmutableMetaDatabase.Builder parentBuilder,
            MutableMetaCollection newCol, MetaElementState newState) throws UnmergeableException {

        ImmutableMetaCollection byName = oldStructure.getMetaCollectionByName(newCol.getName());
        ImmutableMetaCollection byId = oldStructure.getMetaCollectionByIdentifier(newCol.getIdentifier());

        switch (newState) {
            case NOT_CHANGED:
            case NOT_EXISTENT:
                throw new AssertionError("A modification was expected, but the new state is " + newState);
            case ADDED:
            case MODIFIED: {
                if (byName != byId) {
                    throw createUnmergeableException(oldStructure, newCol, byName, byId);
                }
                if (byName == null && byId == null) {
                    parentBuilder.put(newCol.immutableCopy());
                    return ;
                }
                assert byName != null;
                assert byId != null;

                ImmutableMetaCollection.Builder childBuilder = new ImmutableMetaCollection.Builder(byId);
                for (MutableMetaDocPart modifiedDocPart : newCol.getModifiedMetaDocParts()) {
                    merge(newStructure, oldStructure, newCol, byId, childBuilder, modifiedDocPart);
                }
                for (Tuple2<MutableMetaIndex, MetaElementState> modifiedIndex : newCol.getModifiedMetaIndexes()) {
                    merge(oldStructure, newCol, byId, childBuilder, modifiedIndex.v1(), modifiedIndex.v2());
                }
                parentBuilder.put(childBuilder);

                break;
            }
            case REMOVED: {
                if (byName != byId) {
                    /*
                     * The backend transaction will remove by id, but it is referencing another name
                     * on the current snapshot, so the final state will be inconsistent.
                     * It is better to fail.
                     */
                    throw createUnmergeableException(oldStructure, newCol, byName, byId);
                }
                if (byName == null && byId == null) { 
                    /*
                     * it has been removed on another transaction or created and removed on the 
                     * current one. No change must be done
                     */
                    return ;
                }
                assert byName != null;
                assert byId != null;
                /*
                 * In this case, we can delegate on the backend transaction check. If it thinks
                 * everything is fine, we can remove the element. If it thinks there is an error,
                 * then we have to rollback the transaction.
                 */
                parentBuilder.remove(byName);
            }
        }

    }

    private void merge(MetaDatabase newDb, MetaDatabase oldDb, MutableMetaCollection newStructure, ImmutableMetaCollection oldStructure,
            ImmutableMetaCollection.Builder parentBuilder, MutableMetaDocPart changed) throws UnmergeableException {
        ImmutableMetaDocPart byRef = oldStructure.getMetaDocPartByTableRef(changed.getTableRef());
        ImmutableMetaDocPart byId = oldStructure.getMetaDocPartByIdentifier(changed.getIdentifier());

        if (byRef != byId) {
            throw createUnmergeableException(oldDb, oldStructure, changed, byRef, byId);
        }
        if (byRef == null && byId == null) {
            parentBuilder.put(changed.immutableCopy());
            return ;
        }
        assert byRef != null;
        assert byId != null;

        ImmutableMetaDocPart.Builder childBuilder = new ImmutableMetaDocPart.Builder(byId);

        for (ImmutableMetaField addedMetaField : changed.getAddedMetaFields()) {
            merge(oldDb, newStructure, oldStructure, changed, byId, childBuilder, addedMetaField);
        }
        for (ImmutableMetaScalar addedMetaScalar : changed.getAddedMetaScalars()) {
            merge(oldDb, oldStructure, byId, childBuilder, addedMetaScalar);
        }
        for (Tuple2<MutableMetaDocPartIndex, MetaElementState> addedMetaDocPartIndex : changed.getModifiedMetaDocPartIndexes()) {
            merge(oldDb, newStructure, oldStructure, changed, byId, childBuilder, addedMetaDocPartIndex.v1(), addedMetaDocPartIndex.v2());
        }

        parentBuilder.put(childBuilder);

    }

    private void merge(MetaDatabase oldDb, MutableMetaCollection newCol, MetaCollection oldCol, MutableMetaDocPart newStructure, ImmutableMetaDocPart oldStructure,
            ImmutableMetaDocPart.Builder parentBuilder, ImmutableMetaField changed) throws UnmergeableException {
        ImmutableMetaField byNameAndType = oldStructure.getMetaFieldByNameAndType(changed.getName(), changed.getType());
        ImmutableMetaField byId = oldStructure.getMetaFieldByIdentifier(changed.getIdentifier());

        if (byNameAndType != byId) {
            throw createUnmergeableException(oldDb, oldCol, oldStructure, changed, byNameAndType, byId);
        }
        
        if (byNameAndType == null && byId == null) {
            Optional<? extends MetaIndex> oldMissedIndex = oldCol.streamContainedMetaIndexes()
                .filter(oldIndex -> oldIndex.getMetaIndexFieldByTableRefAndName(oldStructure.getTableRef(), changed.getName()) != null &&
                        (newCol.getMetaIndexByName(oldIndex.getName()) == null ||
                        Seq.seq(oldIndex.iteratorMetaDocPartIndexesIdentifiers(newStructure))
                                .filter(identifiers -> identifiers.contains(changed.getIdentifier()))
                                .anyMatch(identifiers -> newStructure.streamIndexes()
                                        .noneMatch(newDocPartIndex -> oldIndex.isMatch(newStructure, identifiers, newDocPartIndex)))))
                .findAny();
            
            if (oldMissedIndex.isPresent()) {
                throw createUnmergeableExceptionForMissing(oldDb, oldCol, oldStructure, changed, oldMissedIndex.get());
            }
            
            parentBuilder.put(changed);
        }
    }

    private void merge(MetaDatabase db, MetaCollection col, ImmutableMetaDocPart oldStructure, 
            ImmutableMetaDocPart.Builder parentBuilder, ImmutableMetaScalar changed) {
        MetaScalar byId = oldStructure.getScalar(changed.getIdentifier());
        MetaScalar byType = oldStructure.getScalar(changed.getType());

        if (byType != byId) {
            throw createUnmergeableException(db, col, oldStructure, changed, byType, byId);
        }
        if (byType == null && byId == null) {
            parentBuilder.put(changed);
        }
    }

    private void merge(MetaDatabase oldDb, MutableMetaCollection newCol, ImmutableMetaCollection oldCol, MetaDocPart newStructure, ImmutableMetaDocPart oldStructure,
            ImmutableMetaDocPart.Builder parentBuilder, MutableMetaDocPartIndex changed, MetaElementState newState) throws UnmergeableException {
        ImmutableMetaDocPartIndex byId = oldStructure.getMetaDocPartIndexByIdentifier(changed.getIdentifier());
        ImmutableMetaDocPartIndex bySameColumns = oldStructure.streamIndexes()
                .filter(oldDocPartIndex -> oldDocPartIndex.hasSameColumns(changed))
                .findAny()
                .orElse(null);

        switch (newState) {
            case NOT_CHANGED:
            case NOT_EXISTENT:
                throw new AssertionError("A modification was expected, but the new state is " + newState);
            case ADDED:
            case MODIFIED: {
                Optional<? extends MetaIndex> anyNewRelatedIndex = 
                        Seq.seq(newCol.getModifiedMetaIndexes())
                            .map(modIndex -> modIndex.v1())
                            .filter(newIndex -> newIndex.isCompatible(newStructure, changed))
                            .findAny();
                
                if (!anyNewRelatedIndex.isPresent()) {
                    Optional<ImmutableMetaIndex> anyOldRelatedIndex = oldCol.streamContainedMetaIndexes()
                            .filter(oldIndex -> oldIndex.isCompatible(newStructure, changed))
                            .findAny();
                    if (!anyOldRelatedIndex.isPresent()) {
                        throw createUnmergeableExceptionForOrphan(oldDb, oldCol, oldStructure, changed);
                    }
                }

                if (byId == null) {
                    parentBuilder.put(changed.immutableCopy());
                    return ;
                }
                assert byId != null;

                ImmutableMetaDocPartIndex.Builder childBuilder = new ImmutableMetaDocPartIndex.Builder(byId);

                for (ImmutableMetaDocPartIndexColumn addedMetaFieldIndex : changed.getAddedMetaDocPartIndexColumns()) {
                    merge(oldDb, oldCol, oldStructure, byId, childBuilder, addedMetaFieldIndex);
                }

                parentBuilder.put(childBuilder);

                break;
            }
            case REMOVED: {
                Optional<? extends MetaIndex> oldMissedIndex = oldCol.streamContainedMetaIndexes()
                        .flatMap(oldIndex -> oldIndex.streamTableRefs()
                                .map(tableRef -> oldCol.getMetaDocPartByTableRef(tableRef))
                                .filter(oldDocPart -> oldDocPart != null &&
                                        oldIndex.isCompatible(oldDocPart, changed) &&
                                        Seq.seq(newCol.getModifiedMetaIndexes())
                                            .noneMatch(newIndex -> newIndex.v2() == MetaElementState.REMOVED &&
                                                    newIndex.v1().getName().equals(oldIndex.getName())))
                                .map(tableRef -> oldIndex))
                        .findAny();
                
                if (oldMissedIndex.isPresent()) {
                    throw createUnmergeableExceptionForMissing(oldDb, oldCol, oldStructure, changed, oldMissedIndex.get());
                }
                
                if (byId == null || bySameColumns == null) {
                    /*
                     * it has been removed on another transaction or created and removed on the 
                     * current one. No change must be done
                     */
                    return ;
                }
                assert byId != null;
                assert bySameColumns != null;
                /*
                 * In this case, we can delegate on the backend transaction check. If it thinks
                 * everything is fine, we can remove the element. If it thinks there is an error,
                 * then we have to rollback the transaction.
                 */
                parentBuilder.remove(byId);
            }
        }
    }

    private void merge(MetaDatabase db, MetaCollection col, MetaDocPart docPart, ImmutableMetaDocPartIndex oldStructure,
            ImmutableMetaDocPartIndex.Builder parentBuilder, ImmutableMetaDocPartIndexColumn changed) throws UnmergeableException {
        ImmutableMetaDocPartIndexColumn byIdentifier = oldStructure.getMetaDocPartIndexColumnByIdentifier(changed.getIdentifier());
        ImmutableMetaDocPartIndexColumn byPosition = oldStructure.getMetaDocPartIndexColumnByPosition(changed.getPosition());

        if (byIdentifier != byPosition) {
            throw createUnmergeableException(db, col, docPart, oldStructure, changed, byIdentifier, byPosition);
        }
        if (byIdentifier == null && byPosition == null) {
            parentBuilder.add(changed);
        }
    }

    private void merge(MetaDatabase oldDb, MutableMetaCollection newStructure, ImmutableMetaCollection oldStructure,
            ImmutableMetaCollection.Builder parentBuilder, MutableMetaIndex changed, MetaElementState newState) throws UnmergeableException {
        ImmutableMetaIndex byName = oldStructure.getMetaIndexByName(changed.getName());

        switch (newState) {
            case NOT_CHANGED:
            case NOT_EXISTENT:
                throw new AssertionError("A modification was expected, but the new state is " + newState);
            case ADDED:
            case MODIFIED: {
                Optional<? extends MetaIndex> anyConflictingIndex = oldStructure.streamContainedMetaIndexes()
                        .filter(index -> index.isMatch(changed) &&
                                Seq.seq(newStructure.getModifiedMetaIndexes())
                                    .noneMatch(modifiedIndex -> modifiedIndex.v2() == MetaElementState.REMOVED &&
                                            modifiedIndex.v1().getName().equals(index.getName())))
                        .findAny();
                
                if (anyConflictingIndex.isPresent()) {
                    throw createUnmergeableException(oldDb, oldStructure, changed, anyConflictingIndex.get());
                }
                
                Optional<? extends MetaDocPart> anyDocPartWithMissingDocPartIndex = changed.streamTableRefs()
                    .map(tableRef -> oldStructure.getMetaDocPartByTableRef(tableRef))
                    .filter(docPart -> docPart != null && changed.isCompatible(docPart) &&
                            Seq.seq(changed.iteratorMetaDocPartIndexesIdentifiers(docPart))
                                .filter(identifiers -> docPart.streamIndexes()
                                        .noneMatch(docPartIndex -> changed.isMatch(docPart, identifiers, docPartIndex)))
                                .anyMatch(identifiers -> {
                                    MutableMetaDocPart newDocPart = newStructure.getMetaDocPartByTableRef(docPart.getTableRef()); 
                                    return Seq.seq(newDocPart.getModifiedMetaDocPartIndexes())
                                            .filter(docPartIndex -> docPartIndex.v2() != MetaElementState.REMOVED)
                                            .noneMatch(docPartIndex -> changed.isMatch(newDocPart, identifiers, docPartIndex.v1()));
                                }))
                    .findAny();
                
                if (anyDocPartWithMissingDocPartIndex.isPresent()) {
                    throw createUnmergeableExceptionForMissing(oldDb, oldStructure, changed, anyDocPartWithMissingDocPartIndex.get());
                }
                
                if (byName == null) {
                    parentBuilder.put(changed.immutableCopy());
                    return ;
                }
                assert byName != null;

                ImmutableMetaIndex.Builder childBuilder = new ImmutableMetaIndex.Builder(byName);

                for (ImmutableMetaIndexField addedMetaIndexField : changed.getAddedMetaIndexFields()) {
                    merge(oldDb, oldStructure, byName, childBuilder, addedMetaIndexField);
                }

                parentBuilder.put(childBuilder);

                break;
            }
            case REMOVED: {
                Optional<? extends MetaDocPartIndex> orphanDocPartIndex = changed.streamTableRefs()
                    .map(tableRef -> (MetaDocPart) oldStructure.getMetaDocPartByTableRef(tableRef))
                    .filter(docPart -> docPart != null && changed.isCompatible(docPart))
                    .flatMap(oldDocPart -> oldDocPart.streamIndexes()
                            .filter(oldDocPartIndex -> changed.isCompatible(oldDocPart, oldDocPartIndex) &&
                                    Seq.seq(newStructure.getMetaDocPartByTableRef(oldDocPart.getTableRef()).getModifiedMetaDocPartIndexes())
                                        .noneMatch(newDocPartIndex -> newDocPartIndex.v2() == MetaElementState.REMOVED &&
                                            newDocPartIndex.v1().getIdentifier().equals(oldDocPartIndex.getIdentifier())) &&
                                    oldStructure.streamContainedMetaIndexes()
                                            .noneMatch(oldIndex -> oldIndex.isCompatible(oldDocPart, oldDocPartIndex) &&
                                                    Seq.seq(newStructure.getModifiedMetaIndexes())
                                                        .noneMatch(newIndex -> newIndex.v2() == MetaElementState.REMOVED &&
                                                            newIndex.v1().getName().equals(oldIndex.getName()))
                                            )
                            )
                    )
                    .findAny();
                
                if (orphanDocPartIndex.isPresent()) {
                    throw createUnmergeableExceptionForOrphan(oldDb, oldStructure, changed, orphanDocPartIndex.get());
                }

                if (byName == null) { 
                    /*
                     * it has been removed on another transaction or created and removed on the 
                     * current one. No change must be done
                     */
                    return ;
                }
                assert byName != null;
                
                /*
                 * In this case, we can delegate on the backend transaction check. If it thinks
                 * everything is fine, we can remove the element. If it thinks there is an error,
                 * then we have to rollback the transaction.
                 */
                parentBuilder.remove(byName);
            }
        }
    }

    private void merge(MetaDatabase db, MetaCollection col, ImmutableMetaIndex oldStructure,
            ImmutableMetaIndex.Builder parentBuilder, ImmutableMetaIndexField changed) throws UnmergeableException {
        ImmutableMetaIndexField byTableRefAndName = oldStructure.getMetaIndexFieldByTableRefAndName(changed.getTableRef(), changed.getName());
        ImmutableMetaIndexField byPosition = oldStructure.getMetaIndexFieldByPosition(changed.getPosition());

        if (byTableRefAndName != byPosition) {
            throw createUnmergeableException(db, col, oldStructure, changed, byTableRefAndName, byPosition);
        }
        if (byTableRefAndName == null && byPosition == null) {
            parentBuilder.add(changed);
        }
    }

    private UnmergeableException createUnmergeableException(
            MetaDatabase newDb, ImmutableMetaDatabase byName, ImmutableMetaDatabase byId) {

        if (byName != null) {
            throw new UnmergeableException(oldSnapshot, newSnapshot,
                    "There is a previous database whose name is " + byName.getName()
                    + " that has a different id. The previous element id is "
                    + byName.getIdentifier() + " and the new one is " + newDb.getIdentifier());
        } else {
            assert byId != null;
            throw new UnmergeableException(oldSnapshot, newSnapshot,
                    "There is a previous database whose id is " + byId.getIdentifier()
                    + " that has a different name. The previous element name is "
                    + byId.getName() + " and the new one is " + newDb.getName());
        }
    }

    private UnmergeableException createUnmergeableException(
            MetaDatabase db,
            MutableMetaCollection newCol, ImmutableMetaCollection byName, ImmutableMetaCollection byId) {

        if (byName != null) {
            throw new UnmergeableException(oldSnapshot, newSnapshot,
                    "There is a previous collection on " + db + " whose name is "
                    + byName.getName() + " that has a different id. The previous "
                    + "element id is " + byName.getIdentifier() + " and the new one is "
                    + newCol.getIdentifier());
        } else {
            assert byId != null;
            throw new UnmergeableException(oldSnapshot, newSnapshot,
                    "There is a previous collection on " + db + " whose id is "
                    + byId.getIdentifier() + " that has a different name. The previous "
                    + "element name is " + byId.getName() + " and the new one is "
                    + newCol.getIdentifier());
        }
    }

    private UnmergeableException createUnmergeableException(
            MetaDatabase db,
            MetaCollection col, MutableMetaDocPart changed, ImmutableMetaDocPart byRef,
            ImmutableMetaDocPart byId) {
        if (byRef != null) {
            throw new UnmergeableException(oldSnapshot, newSnapshot,
                    "There is a previous doc part on " + db + "." + col + " whose ref is "
                    + byRef.getTableRef() + " that has a different id. The previous "
                    + "element id is " + byRef.getIdentifier() + " and the new one is "
                    + changed.getIdentifier());
        } else {
            assert byId != null;
            throw new UnmergeableException(oldSnapshot, newSnapshot,
                    "There is a previous doc part on " + db + "." + col + " whose id is "
                    + byId.getIdentifier() + " that has a different ref. The previous "
                    + "element ref is " + byId.getTableRef() + " and the new one is "
                    + changed.getTableRef());
        }
    }

    private UnmergeableException createUnmergeableExceptionForMissing(
            MetaDatabase db,
            MetaCollection col, MetaDocPart docPart, MetaField changed, MetaIndex oldMissedIndex) {
        throw new UnmergeableException(oldSnapshot, newSnapshot,
                "There is a previous index on " + db + "." + col 
                + " whose name is " + oldMissedIndex.getName()
                + " associated with new doc part field " +  docPart + "."
                + changed + " and the corresponding doc part index has not been created.");
    }

    private UnmergeableException createUnmergeableException(
            MetaDatabase db, MetaCollection col, MetaDocPart docPart, ImmutableMetaField changed,
            ImmutableMetaField byNameAndType, ImmutableMetaField byId) {
        if (byNameAndType != null) {
            throw new UnmergeableException(oldSnapshot, newSnapshot,
                    "There is a previous field on doc part " + db + "." + col + "." + docPart + " whose "
                    + "name is " + byNameAndType.getName() + " and type is "
                    + byNameAndType.getType() + " that has a different id. The previous "
                    + "element id is " + byNameAndType.getIdentifier() + " and the new "
                    + "one is " + changed.getIdentifier());
        } else {
            assert byId != null;
            throw new UnmergeableException(oldSnapshot, newSnapshot,
                    "There is a previous field on doc part " + db.getIdentifier() + "." + col.getIdentifier()
                    + "." + docPart.getIdentifier() + " whose id is " + byId.getIdentifier() + " that has a different name or "
                    + "type. The previous element name is " + byId.getName() + " and its "
                    + "type is " + byId.getType() + ". The name of the new one is "
                    + changed.getName() + " and its type is " + changed.getType());
        }
    }

    private UnmergeableException createUnmergeableException(
            MetaDatabase db, MetaCollection col, MetaDocPart docPart, ImmutableMetaScalar changed,
            MetaScalar byType, MetaScalar byId) {
        if (byType != null) {
            throw new UnmergeableException(oldSnapshot, newSnapshot,
                    "There is a previous meta scalar on " + db.getIdentifier() + "."
                    + col.getIdentifier() + "." + docPart.getIdentifier() + " whose "
                    + "type is " + changed.getType() + " but its identifier is "
                    + byType.getIdentifier() + ". The identifier of the new one is "
                    + changed.getIdentifier()
            );
        } else {
            assert byId != null;
            throw new UnmergeableException(oldSnapshot, newSnapshot,
                    "There is a previous meta scalar on " + db.getIdentifier() + "."
                    + col.getIdentifier() + "." + docPart.getIdentifier() + " whose "
                    + "identifier is " + changed.getIdentifier() + " but its type is "
                    + byId.getType() + ". The type of the new one is " + changed.getType()
            );
        }
    }

    private UnmergeableException createUnmergeableExceptionForOrphan(
            MetaDatabase db, MetaCollection col, MetaDocPart docPart, MetaDocPartIndex changed) {
        throw new UnmergeableException(oldSnapshot, newSnapshot,
                "There is a new doc part index " + db.getIdentifier() + "." + col.getIdentifier() + 
                "." + docPart.getIdentifier() + "." + changed.getIdentifier()
                + " that has no index associated");
    }

    private UnmergeableException createUnmergeableExceptionForMissing(
            MetaDatabase db, MetaCollection col, MetaDocPart docPart, MetaDocPartIndex changed,
            MetaIndex oldMissedIndex) {
        throw new UnmergeableException(oldSnapshot, newSnapshot,
                "There is a previous doc part index " + db + "." + col + 
                "." + docPart + "." + oldMissedIndex
                + " that is compatible with the removed doc part index "
                + db.getIdentifier() + "." + col.getIdentifier() + 
                "." + docPart.getIdentifier() + "." + changed.getIdentifier());
    }

    private UnmergeableException createUnmergeableException(
            MetaDatabase db, MetaCollection col, MetaDocPart docPart, MetaDocPartIndex index, ImmutableMetaDocPartIndexColumn changed,
            ImmutableMetaDocPartIndexColumn byIdentifier, ImmutableMetaDocPartIndexColumn byPosition) {
        if (byIdentifier != null) {
            throw new UnmergeableException(oldSnapshot, newSnapshot,
                    "There is a previous field on doc part index " + db.getIdentifier() + "." + col.getIdentifier() + 
                    "." + docPart.getIdentifier() + "." + index.getIdentifier() + " whose "
                    + "identifier is " + byIdentifier.getIdentifier() 
                    + " that has a different position. The previous "
                    + "element position is " + byIdentifier.getPosition() + " and the new "
                    + "one is " + changed.getPosition());
        } else {
            assert byPosition != null;
            throw new UnmergeableException(oldSnapshot, newSnapshot,
                    "There is a previous field on doc part index " + db.getIdentifier() + "." + col.getIdentifier()
                    + "." + docPart.getIdentifier() + "." + index.getIdentifier() + 
                    " whose position is " + byPosition.getPosition() + " that has a different name or "
                    + "type. The previous element identifier is " + byPosition.getIdentifier()
                    + ". The name of the new one is " + changed.getIdentifier());
        }
    }

    private UnmergeableException createUnmergeableException(
            MetaDatabase db, MetaCollection col, MetaIndex index, ImmutableMetaIndexField changed,
            ImmutableMetaIndexField byTableRefAndName, ImmutableMetaIndexField byPosition) {
        if (byTableRefAndName != null) {
            throw new UnmergeableException(oldSnapshot, newSnapshot,
                    "There is a previous field on index " + db + "." + col + "." + index + " whose "
                    + "tableRef is " + byTableRefAndName.getTableRef() + " and name is "
                    + byTableRefAndName.getName() + " that has a different position. The previous "
                    + "element position is " + byTableRefAndName.getPosition() + " and the new "
                    + "one is " + changed.getPosition());
        } else {
            assert byPosition != null;
            throw new UnmergeableException(oldSnapshot, newSnapshot,
                    "There is a previous field on index " + db + "." + col
                    + "." + index + " whose position is " + byPosition.getPosition() + " that has a different tableRef or "
                    + "name. The previous element tableRef is " + byPosition.getTableRef() + " and its "
                    + "name is " + byPosition.getName() + ". The tableRef of the new one is "
                    + changed.getTableRef() + " and its name is " + changed.getName());
        }
    }

    private UnmergeableException createUnmergeableException(
            MetaDatabase db,
            MetaCollection col, MutableMetaIndex changed, MetaIndex oldIndex) {
        throw new UnmergeableException(oldSnapshot, newSnapshot,
                "There is a previous index on " + db + "." + col + "." + oldIndex 
                + " that conflict with new index "
                + changed);
    }

    private UnmergeableException createUnmergeableExceptionForOrphan(
            MetaDatabase db,
            MetaCollection col, MutableMetaIndex changed, MetaDocPartIndex oldOrphanDocPartIndex) {
        throw new UnmergeableException(oldSnapshot, newSnapshot,
                "There is a previous doc part index on " + db.getIdentifier() + "." + oldOrphanDocPartIndex.getIdentifier()
                + " associated only with removed index "
                + changed + " that has not been deleted.");
    }

    private UnmergeableException createUnmergeableExceptionForMissing(
            MetaDatabase db,
            MetaCollection col, MutableMetaIndex changed, MetaDocPart oldDocPartForMissedIndex) {
        throw new UnmergeableException(oldSnapshot, newSnapshot,
                "There should be a doc part index on " + db + "." + col + "." + oldDocPartForMissedIndex 
                + " associated only with new index "
                + changed + " that has not been created.");
    }

}
