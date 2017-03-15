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

package com.torodb.metainfo.cache.mvcc;

import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPartIndexColumn;
import com.torodb.core.transaction.metainf.ImmutableMetaField;
import com.torodb.core.transaction.metainf.ImmutableMetaIdentifiedDocPartIndex;
import com.torodb.core.transaction.metainf.ImmutableMetaIndex;
import com.torodb.core.transaction.metainf.ImmutableMetaIndexField;
import com.torodb.core.transaction.metainf.ImmutableMetaScalar;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaElementState;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaIdentifiedDocPartIndex;
import com.torodb.core.transaction.metainf.MetaIndex;
import com.torodb.core.transaction.metainf.MetaScalar;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import com.torodb.core.transaction.metainf.MutableMetaIndex;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import com.torodb.core.transaction.metainf.UnmergeableException;
import com.torodb.metainfo.cache.mvcc.merge.MergeStrategy;
import com.torodb.metainfo.cache.mvcc.merge.docpartindex.DocPartIndexCtx;
import com.torodb.metainfo.cache.mvcc.merge.docpartindex.MissedDocIndexStrategy;
import com.torodb.metainfo.cache.mvcc.merge.docpartindex.NoDocIndexStrategy;
import com.torodb.metainfo.cache.mvcc.merge.field.FieldContext;
import com.torodb.metainfo.cache.mvcc.merge.field.MissingIndexStrategy;
import com.torodb.metainfo.cache.mvcc.merge.index.ConflictingIndexStrategy;
import com.torodb.metainfo.cache.mvcc.merge.index.IndexContext;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Optional;

/**
 * The old {@link SnapshotMerger} version that does not delegate on
 * {@link MergeStrategy strategies}.
 *
 * It is not directly used, but called by the actual merger to verify that they implement the same
 * logic.
 */
@Deprecated
public class LegacySnapshotMerger {

  private final ImmutableMetaSnapshot oldSnapshot;
  private final MutableMetaSnapshot newSnapshot;

  public LegacySnapshotMerger(ImmutableMetaSnapshot oldSnapshot, MutableMetaSnapshot newSnapshot) {
    this.oldSnapshot = oldSnapshot;
    this.newSnapshot = newSnapshot;
  }

  public ImmutableMetaSnapshot.Builder merge() throws UnmergeableException {
    ImmutableMetaSnapshot.Builder builder = new ImmutableMetaSnapshot.Builder(oldSnapshot);
    newSnapshot.streamModifiedDatabases().forEach(change ->
        mergeDb(builder, change.getElement(), change.getChange())
    );
    return builder;
  }

  /**
   * Merge changes on database level.
   *
   * @param parentBuilder a snapshot builder that contains the state of the affected database on the
   *                      last commited snapshot
   * @param newDb         the database with modifications.
   * @param newState      the new state of the given database
   * @throws UnmergeableException if there are incompatible changes between versions
   */
  private void mergeDb(ImmutableMetaSnapshot.Builder parentBuilder, MutableMetaDatabase newDb,
      MetaElementState newState) throws UnmergeableException {

    //the database on the old snapshot that has the name of the modified database
    ImmutableMetaDatabase oldByName = oldSnapshot.getMetaDatabaseByName(newDb.getName());
    //the database on the new snapshot that has the id of the modified database
    ImmutableMetaDatabase oldById = oldSnapshot.getMetaDatabaseByIdentifier(newDb.getIdentifier());

    switch (newState) {
      default:
      case NOT_CHANGED:
      case NOT_EXISTENT:
        throw new AssertionError("A modification was expected, but the new state is " + newState);
      case ADDED:
      case MODIFIED: {
        if (oldByName != oldById) {
          //implemented by SameIdOtherName and SameNameOtherId
          throw createUnmergeableException(newDb, oldByName, oldById);
        }
        if (oldByName == null && oldById == null) { //the old database didn't exist.
          //implemented by NewDatabaseStrategy
          parentBuilder.put(newDb.immutableCopy());
          return;
        }
        assert oldByName != null;
        assert oldById != null;

        ImmutableMetaDatabase.Builder childBuilder = new ImmutableMetaDatabase.Builder(oldById);
        newDb.streamModifiedCollections().forEach(modifiedCollection ->
            //implemented by DatabaseModifiedChildrenStrategy
            mergeCol(newDb, oldById, childBuilder,
                modifiedCollection.getElement(), modifiedCollection.getChange())
        );
        //implemented by DatabaseModifiedChildrenStrategy
        parentBuilder.put(childBuilder);
        break;
      }
      case REMOVED: {
        if (oldByName != oldById) {
          //implemented by SameIdOtherName and SameNameOtherId
          /*
           * The backend transaction will remove by id, but it is referencing another name on the
           * current snapshot, so the final state will be inconsistent. It is better to fail.
           */
          throw createUnmergeableException(newDb, oldByName, oldById);
        }
        if (oldByName == null && oldById == null) {
          //implemented by NotExistentDatabaseStrategy
          /*
           * it has been removed on another transaction or created and removed on the current one.
           * No change must be done
           */
          return;
        }
        assert oldByName != null;
        assert oldById != null;
        //implemented by DatabaseMergeStrategy
        /*
         * In this case, we can delegate on the backend transaction check. If it thinks everything
         * is fine, we can remove the element. If it thinks there is an error, then we have to
         * rollback the transaction.
         */
        parentBuilder.remove(oldByName);
      }
    }

  }

  /**
   * Merge changes on collection level.
   * 
   * @param newStructure the new database
   * @param oldStructure the last commited version of that database
   * @param parentBuilder the database builder that builds the merged structure
   * @param newCol the collection that whose modifications must be added to the builder
   * @param newState the modification type
   */
  private void mergeCol(MetaDatabase newStructure, ImmutableMetaDatabase oldStructure,
      ImmutableMetaDatabase.Builder parentBuilder,
      MutableMetaCollection newCol, MetaElementState newState) throws UnmergeableException {

    ImmutableMetaCollection oldByName = oldStructure.getMetaCollectionByName(newCol.getName());
    ImmutableMetaCollection oldById = oldStructure
        .getMetaCollectionByIdentifier(newCol.getIdentifier());

    switch (newState) {
      default:
      case NOT_CHANGED:
      case NOT_EXISTENT:
        throw new AssertionError("A modification was expected, but the new state is " + newState);
      case ADDED:
      case MODIFIED: {
        if (oldByName != oldById) {
          //implemented by SameIdOtherName and SameNameOtherId
          throw createUnmergeableException(oldStructure, newCol, oldByName, oldById);
        }
        if (oldByName == null && oldById == null) {
          //implemented by NewCollectionStrategy
          parentBuilder.put(newCol.immutableCopy());
          return;
        }
        assert oldByName != null;
        assert oldById != null;

        ImmutableMetaCollection.Builder childBuilder = new ImmutableMetaCollection.Builder(oldById);
        newCol.streamModifiedMetaDocParts().forEach(modifiedDocPart ->
            mergeDocPart(newStructure, oldStructure, newCol, oldById, childBuilder, modifiedDocPart)
        );
        newCol.streamModifiedMetaIndexes().forEach(modifiedIndex ->
            mergeIdx(oldStructure, newCol, oldById, childBuilder,
                modifiedIndex.getElement(), modifiedIndex.getChange()
            )
        );
        parentBuilder.put(childBuilder);

        break;
      }
      case REMOVED: {
        if (oldByName != oldById) {
          //implemented by SameIdOtherName and SameNameOtherId
          /*
           * The backend transaction will remove by id, but it is referencing another name on the
           * current snapshot, so the final state will be inconsistent. It is better to fail.
           */
          throw createUnmergeableException(oldStructure, newCol, oldByName, oldById);
        }
        if (oldByName == null && oldById == null) {
          //implemented by NotExistentDatabaseStrategy
          /*
           * it has been removed on another transaction or created and removed on the current one.
           * No change must be done
           */
          return;
        }
        assert oldByName != null;
        assert oldById != null;
        /*
         * In this case, we can delegate on the backend transaction check. If it thinks everything
         * is fine, we can remove the element. If it thinks there is an error, then we have to
         * rollback the transaction.
         */
        parentBuilder.remove(oldByName);
      }
    }

  }

  @SuppressWarnings("checkstyle:LineLength")
  private void mergeDocPart(MetaDatabase newDb, MetaDatabase oldDb, MutableMetaCollection newStructure,
      ImmutableMetaCollection oldStructure,
      ImmutableMetaCollection.Builder parentBuilder, MutableMetaDocPart changed) throws
      UnmergeableException {
    ImmutableMetaDocPart oldByRef = oldStructure.getMetaDocPartByTableRef(changed.getTableRef());
    ImmutableMetaDocPart oldById = oldStructure.getMetaDocPartByIdentifier(changed.getIdentifier());

    if (oldByRef != oldById) {
      throw createUnmergeableException(oldDb, oldStructure, changed, oldByRef, oldById);
    }
    if (oldByRef == null && oldById == null) {
      //implemented by NewDocPartStrategy
      parentBuilder.put(changed.immutableCopy());
      return;
    }
    assert oldByRef != null;
    assert oldById != null;

    ImmutableMetaDocPart.Builder childBuilder = new ImmutableMetaDocPart.Builder(oldById);

    changed.streamAddedMetaFields().forEach(addedMetaField ->
        //Implemented by DocPartRecurrentStrategy
        mergeField(oldDb, newStructure, oldStructure, changed, oldById, childBuilder,
            addedMetaField)
    );
    changed.streamAddedMetaScalars().forEach(addedMetaScalar ->
        //Implemented by DocPartRecurrentStrategy
        mergeScalar(oldDb, oldStructure, oldById, childBuilder, addedMetaScalar)
    );
    changed.streamModifiedMetaDocPartIndexes().forEach(addedMetaDocPartIndex ->
        //Implemented by DocPartRecurrentStrategy
        mergeDocPartIdx(oldDb, newStructure, oldStructure, changed, oldById, childBuilder,
            addedMetaDocPartIndex.getElement(), addedMetaDocPartIndex.getChange())
    );

    parentBuilder.put(childBuilder);

  }

  private void mergeField(MetaDatabase oldDb, MutableMetaCollection newCol, 
      ImmutableMetaCollection oldCol,
      MutableMetaDocPart newStructure, ImmutableMetaDocPart oldStructure,
      ImmutableMetaDocPart.Builder parentBuilder, ImmutableMetaField changed) throws
      UnmergeableException {
    ImmutableMetaField byNameAndType = oldStructure.getMetaFieldByNameAndType(changed.getName(),
        changed.getType());
    ImmutableMetaField byId = oldStructure.getMetaFieldByIdentifier(changed.getIdentifier());

    if (byNameAndType != byId) {
      throw createUnmergeableException(oldDb, oldCol, oldStructure, changed, byNameAndType, byId);
    }

    if (byNameAndType == null && byId == null) {
      FieldContext ctx = new FieldContext(oldStructure, changed, newStructure, oldCol,
          newCol);

      Optional<? extends MetaIndex> oldMissedIndex = MissingIndexStrategy.getAnyMissedIndex(ctx);

      if (oldMissedIndex.isPresent()) {
        throw createUnmergeableExceptionForMissing(oldDb, oldCol, oldStructure, changed,
            oldMissedIndex.get());
      }

      //implemented by NewFieldStrategy
      parentBuilder.put(changed);
    }
  }

  private void mergeScalar(MetaDatabase db, MetaCollection col, ImmutableMetaDocPart oldStructure,
      ImmutableMetaDocPart.Builder parentBuilder, ImmutableMetaScalar changed) {
    MetaScalar byId = oldStructure.getScalar(changed.getIdentifier());
    MetaScalar byType = oldStructure.getScalar(changed.getType());

    if (byType != byId) {
      throw createUnmergeableException(db, col, oldStructure, changed, byType, byId);
    }
    if (byType == null && byId == null) {
      //implemented by NewFieldStrategy
      parentBuilder.put(changed);
    }
  }

  @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
  private void mergeDocPartIdx(MetaDatabase oldDb, MutableMetaCollection newCol,
      ImmutableMetaCollection oldCol, MutableMetaDocPart newStructure, 
      ImmutableMetaDocPart oldStructure, ImmutableMetaDocPart.Builder parentBuilder,
      ImmutableMetaIdentifiedDocPartIndex changed, MetaElementState newState)
      throws UnmergeableException {
    ImmutableMetaIdentifiedDocPartIndex byId = oldStructure.getMetaDocPartIndexByIdentifier(changed
        .getIdentifier());

    switch (newState) {
      default:
      case NOT_CHANGED:
      case NOT_EXISTENT:
        throw new AssertionError("A modification was expected, but the new state is " + newState);
      case ADDED:
      case MODIFIED: {
        DocPartIndexCtx ctx = new DocPartIndexCtx(
            oldStructure,
            changed,
            MetaElementState.ADDED,
            newStructure,
            oldCol,
            newCol
        );
        Optional<? extends MetaIndex> anyRelatedIndex = NoDocIndexStrategy.getAnyRelatedIndex(ctx);

        if (!anyRelatedIndex.isPresent()) {
          //Implemented by NoDocIndexStrategy
          throw createUnmergeableExceptionForOrphan(oldDb, oldCol, oldStructure, changed);
        }

        if (byId == null) {
          //implemented by NewDocPartIndexStrategy
          parentBuilder.put(changed);
          return;
        }
        assert byId != null;

        ImmutableMetaIdentifiedDocPartIndex.Builder childBuilder =
            new ImmutableMetaIdentifiedDocPartIndex.Builder(byId);

        //implemented by DocPartIndexChildrenStrategy
        changed.streamColumns().forEach(indexColumn ->
            mergeIndexColumn(oldDb, oldCol, oldStructure, byId, childBuilder,
                indexColumn.immutableCopy())
        );

        //implemented by DocPartIndexChildrenStrategy
        parentBuilder.put(childBuilder);

        break;
      }
      case REMOVED: {
        DocPartIndexCtx ctx = new DocPartIndexCtx(oldStructure, changed, MetaElementState.ADDED,
            newStructure, oldCol, newCol);
        Optional<? extends MetaIndex> oldMissedIndex
            = MissedDocIndexStrategy.getAnyMissedIndex(ctx);

        if (oldMissedIndex.isPresent()) {
          //implemented by MissedDocIndexStrategy
          throw createUnmergeableExceptionForMissing(oldDb, oldCol, oldStructure, changed,
              oldMissedIndex.get());
        }

        ImmutableMetaIdentifiedDocPartIndex bySameColumns = oldStructure.streamIndexes()
            .filter(oldDocPartIndex -> oldDocPartIndex.hasSameColumns(changed))
            .findAny()
            .orElse(null);

        if (byId == null || bySameColumns == null) {
          //implemented by NotExistentDocPartIndexStrategy
          /*
           * it has been removed on another transaction or created and removed on the current one.
           * No change must be done
           */
          return;
        }
        assert byId != null;
        assert bySameColumns != null;
        /*
         * In this case, we can delegate on the backend transaction check. If it thinks everything
         * is fine, we can remove the element. If it thinks there is an error, then we have to
         * rollback the transaction.
         */
        parentBuilder.remove(byId);
      }
    }
  }

  private void mergeIndexColumn(MetaDatabase db, MetaCollection col, MetaDocPart docPart,
      ImmutableMetaIdentifiedDocPartIndex oldStructure,
      ImmutableMetaIdentifiedDocPartIndex.Builder parentBuilder,
      ImmutableMetaDocPartIndexColumn changed) throws UnmergeableException {
    ImmutableMetaDocPartIndexColumn byIdentifier = oldStructure
        .getMetaDocPartIndexColumnByIdentifier(changed.getIdentifier());
    ImmutableMetaDocPartIndexColumn byPosition = oldStructure.getMetaDocPartIndexColumnByPosition(
        changed.getPosition());

    if (byIdentifier != byPosition) {
      //implemented by SamePositionOtherIdNameStrategy and SameIdOtherPositionNameStrategy
      throw createUnmergeableException(db, col, docPart, oldStructure, changed, byIdentifier,
          byPosition);
    }
    if (byIdentifier == null && byPosition == null) {
      //implemented by NewIndexColumnStrategy
      parentBuilder.add(changed);
    }
  }

  @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
  private void mergeIdx(MetaDatabase oldDb, MutableMetaCollection newStructure,
      ImmutableMetaCollection oldStructure,
      ImmutableMetaCollection.Builder parentBuilder, MutableMetaIndex changed,
      MetaElementState newState) throws UnmergeableException {
    ImmutableMetaIndex byName = oldStructure.getMetaIndexByName(changed.getName());

    switch (newState) {
      default:
      case NOT_CHANGED:
      case NOT_EXISTENT:
        throw new AssertionError("A modification was expected, but the new state is " + newState);
      case ADDED:
      case MODIFIED: {
        IndexContext ctx = new IndexContext(oldStructure, changed, newState, newStructure);
        Optional<? extends MetaIndex> anyConflictingIndex = ConflictingIndexStrategy
            .getAnyConflictingIndex(ctx);

        if (anyConflictingIndex.isPresent()) {
          //implemented by ConflictingIndexStrategy
          throw createUnmergeableException(oldDb, oldStructure, changed, anyConflictingIndex.get());
        }

        Optional<? extends MetaDocPart> anyDocPartWithMissingDocPartIndex =
            newStructure.getAnyDocPartWithMissedDocPartIndex(oldStructure, changed);

        if (anyDocPartWithMissingDocPartIndex.isPresent()) {
          //implemented by AnyDocPartWithMissedDocPartIndexStrategy
          throw createUnmergeableExceptionForMissing(oldDb, oldStructure, changed,
              anyDocPartWithMissingDocPartIndex.get());
        }

        if (byName == null) {
          //implemented by NewIndexStrategy
          parentBuilder.put(changed.immutableCopy());
          return;
        }
        assert byName != null;

        
        ImmutableMetaIndex.Builder childBuilder = new ImmutableMetaIndex.Builder(byName);
 
        //implemented by IndexChildrenStrategy
        changed.streamAddedMetaIndexFields().forEach(addedMetaIndexField ->
            mergeIndexField(oldDb, oldStructure, byName, childBuilder, addedMetaIndexField)
        );

        parentBuilder.put(childBuilder);

        break;
      }
      case REMOVED: {
        Optional<? extends MetaIdentifiedDocPartIndex> orphanDocPartIndex = newStructure
            .getAnyOrphanDocPartIndex(oldStructure, changed);

        if (orphanDocPartIndex.isPresent()) {
          //implemented by OrphanDocPartIndexStrategy
          throw createUnmergeableExceptionForOrphan(oldDb, oldStructure, changed, orphanDocPartIndex
              .get());
        }

        if (byName == null) {
          //implemented by NotExistentIndexStrategy
          /*
           * it has been removed on another transaction or created and removed on the current one.
           * No change must be done
           */
          return;
        }
        assert byName != null;

        /*
         * In this case, we can delegate on the backend transaction check. If it thinks everything
         * is fine, we can remove the element. If it thinks there is an error, then we have to
         * rollback the transaction.
         */
        parentBuilder.remove(byName);
      }
    }
  }

  private void mergeIndexField(MetaDatabase db, MetaCollection col, ImmutableMetaIndex oldStructure,
      ImmutableMetaIndex.Builder parentBuilder, ImmutableMetaIndexField changed) throws
      UnmergeableException {
    ImmutableMetaIndexField byTableRefAndName = oldStructure.getMetaIndexFieldByTableRefAndName(
        changed.getTableRef(), changed.getFieldName());
    ImmutableMetaIndexField byPosition = oldStructure.getMetaIndexFieldByPosition(changed
        .getPosition());

    if (byTableRefAndName != byPosition) {
      //implemented by samePosition*Strategy and sameRefAndNameOtherPositionStrategy
      throw createUnmergeableException(db, col, oldStructure, changed, byTableRefAndName,
          byPosition);
    }
    if (byTableRefAndName == null && byPosition == null) {
      //implemented by NewIndexFieldStrategy
      parentBuilder.add(changed);
    }
  }

  private UnmergeableException createUnmergeableException(
      MetaDatabase newDb, ImmutableMetaDatabase byName, ImmutableMetaDatabase byId) {

    //implemented by SameIdOtherName and SameNameOtherId
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
    //implemented by SameIdOtherRef and SameRefOtherId

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
    //implemented by SameIdOtherRef and SameRefOtherId
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
          + "." + docPart.getIdentifier() + " whose id is " + byId.getIdentifier()
          + " that has a different name or "
          + "type. The previous element name is " + byId.getName() + " and its "
          + "type is " + byId.getType() + ". The name of the new one is "
          + changed.getName() + " and its type is " + changed.getType());
    }
  }

  private UnmergeableException createUnmergeableException(
      MetaDatabase db, MetaCollection col, MetaDocPart docPart, ImmutableMetaScalar changed,
      MetaScalar byType, MetaScalar byId) {
    //implemented by SameIdOtherRef and SameRefOtherId
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

  private UnmergeableException createUnmergeableException(
      MetaDatabase db, MetaCollection col, MetaDocPart docPart, MetaIdentifiedDocPartIndex index,
      ImmutableMetaDocPartIndexColumn changed,
      ImmutableMetaDocPartIndexColumn byIdentifier, ImmutableMetaDocPartIndexColumn byPosition) {
    if (byIdentifier != null) {
      throw new UnmergeableException(oldSnapshot, newSnapshot,
          "There is a previous field on doc part index " + db.getIdentifier() + "." + col
          .getIdentifier() + "." + docPart.getIdentifier() + "." + index.getIdentifier() + " whose "
          + "identifier is " + byIdentifier.getIdentifier()
          + " that has a different position. The previous "
          + "element position is " + byIdentifier.getPosition() + " and the new "
          + "one is " + changed.getPosition());
    } else {
      assert byPosition != null;
      throw new UnmergeableException(oldSnapshot, newSnapshot,
          "There is a previous field on doc part index " + db.getIdentifier() + "." + col
          .getIdentifier()
          + "." + docPart.getIdentifier() + "." + index.getIdentifier() + " whose position is "
          + byPosition.getPosition() + " that has a different name or "
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
          + byTableRefAndName.getFieldName() + " that has a different position. The previous "
          + "element position is " + byTableRefAndName.getPosition() + " and the new "
          + "one is " + changed.getPosition());
    } else {
      assert byPosition != null;
      throw new UnmergeableException(oldSnapshot, newSnapshot,
          "There is a previous field on index " + db + "." + col
          + "." + index + " whose position is " + byPosition.getPosition()
          + " that has a different tableRef or "
          + "name. The previous element tableRef is " + byPosition.getTableRef() + " and its "
          + "name is " + byPosition.getFieldName() + ". The tableRef of the new one is "
          + changed.getTableRef() + " and its name is " + changed.getFieldName());
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

  private UnmergeableException createUnmergeableExceptionForMissing(
      MetaDatabase db,
      MetaCollection col, MetaDocPart docPart, MetaField changed, MetaIndex oldMissedIndex) {
    throw new UnmergeableException(oldSnapshot, newSnapshot,
        "There is a previous index on " + db + "." + col
        + " whose name is " + oldMissedIndex.getName()
        + " associated with new doc part field " + docPart + "."
        + changed + " and the corresponding doc part index has not been created.");
  }

  private UnmergeableException createUnmergeableExceptionForMissing(
      MetaDatabase db, MetaCollection col, MetaDocPart docPart, MetaIdentifiedDocPartIndex changed,
      MetaIndex oldMissedIndex) {
    throw new UnmergeableException(oldSnapshot, newSnapshot,
        "There is a previous doc part index " + db + "." + col + "." + docPart + "."
        + oldMissedIndex
        + " that is compatible with the removed doc part index "
        + db.getIdentifier() + "." + col.getIdentifier() + "." + docPart.getIdentifier() + "."
        + changed.getIdentifier());
  }

  private UnmergeableException createUnmergeableExceptionForMissing(
      MetaDatabase db,
      MetaCollection col, MutableMetaIndex changed, MetaDocPart oldDocPartForMissedIndex) {
    throw new UnmergeableException(oldSnapshot, newSnapshot,
        "There should be a doc part index on " + db + "." + col + "." + oldDocPartForMissedIndex
        + " associated only with new index "
        + changed + " that has not been created.");
  }

  private UnmergeableException createUnmergeableExceptionForOrphan(
      MetaDatabase db,
      MetaCollection col,
      MetaDocPart docPart,
      MetaIdentifiedDocPartIndex changed) {
    throw new UnmergeableException(oldSnapshot, newSnapshot,
        "There is a new doc part index " + db.getIdentifier() + "." + col.getIdentifier() + "."
        + docPart.getIdentifier() + "." + changed.getIdentifier()
        + " that has no index associated");
  }

  private UnmergeableException createUnmergeableExceptionForOrphan(
      MetaDatabase db,
      MetaCollection col, 
      MutableMetaIndex changed,
      MetaIdentifiedDocPartIndex oldOrphanDocPartIndex) {
    throw new UnmergeableException(oldSnapshot, newSnapshot,
        "There is a previous doc part index on " + db.getIdentifier() + "." + oldOrphanDocPartIndex
        .getIdentifier()
        + " associated only with removed index "
        + changed + " that has not been deleted.");
  }

}
