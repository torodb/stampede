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

import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.ImmutableMetaField;
import com.torodb.core.transaction.metainf.ImmutableMetaScalar;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaElementState;
import com.torodb.core.transaction.metainf.MetaScalar;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import com.torodb.core.transaction.metainf.UnmergeableException;
import org.jooq.lambda.tuple.Tuple2;

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
                    merge(byId, childBuilder, modifiedCollection.v1(), modifiedCollection.v2());
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

    private void merge(ImmutableMetaDatabase oldStructure, ImmutableMetaDatabase.Builder parentBuilder,
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
                    merge(oldStructure, byId, childBuilder, modifiedDocPart);
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

    private void merge(MetaDatabase db, ImmutableMetaCollection oldStructure,
            ImmutableMetaCollection.Builder parentBuilder, MutableMetaDocPart changed) throws UnmergeableException {
        ImmutableMetaDocPart byRef = oldStructure.getMetaDocPartByTableRef(changed.getTableRef());
        ImmutableMetaDocPart byId = oldStructure.getMetaDocPartByIdentifier(changed.getIdentifier());

        if (byRef != byId) {
            throw createUnmergeableException(db, oldStructure, changed, byRef, byId);
        }
        if (byRef == null && byId == null) {
            parentBuilder.put(changed.immutableCopy());
            return ;
        }
        assert byRef != null;
        assert byId != null;

        ImmutableMetaDocPart.Builder childBuilder = new ImmutableMetaDocPart.Builder(byId);

        for (ImmutableMetaField addedMetaField : changed.getAddedMetaFields()) {
            merge(db, oldStructure, byId, childBuilder, addedMetaField);
        }
        for (ImmutableMetaScalar addedMetaScalar : changed.getAddedMetaScalars()) {
            merge(db, oldStructure, byId, childBuilder, addedMetaScalar);
        }

        parentBuilder.put(childBuilder);

    }

    private void merge(MetaDatabase db, MetaCollection col, ImmutableMetaDocPart oldStructure,
            ImmutableMetaDocPart.Builder parentBuilder, ImmutableMetaField changed) throws UnmergeableException {
        ImmutableMetaField byNameAndType = oldStructure.getMetaFieldByNameAndType(changed.getName(), changed.getType());
        ImmutableMetaField byId = oldStructure.getMetaFieldByIdentifier(changed.getIdentifier());

        if (byNameAndType != byId) {
            throw createUnmergeableException(db, col, oldStructure, changed, byNameAndType, byId);
        }
        if (byNameAndType == null && byId == null) {
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

    private UnmergeableException createUnmergeableException(
            MetaDatabase db, MetaCollection col, MetaDocPart docPart, ImmutableMetaField changed,
            ImmutableMetaField byNameAndType, ImmutableMetaField byId) {
        if (byNameAndType != null) {
            throw new UnmergeableException(oldSnapshot, newSnapshot,
                    "There is a previous field on " + db + "." + col + "." + docPart + " whose "
                    + "name is " + byNameAndType.getName() + " and type is "
                    + byNameAndType.getType() + " that has a different id. The previous "
                    + "element id is " + byNameAndType.getIdentifier() + " and the new "
                    + "one is " + changed.getIdentifier());
        } else {
            assert byId != null;
            throw new UnmergeableException(oldSnapshot, newSnapshot,
                    "There is a previous field on " + db.getIdentifier() + "." + col.getIdentifier()
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

}
