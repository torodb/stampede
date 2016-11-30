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

import static org.mockito.Mockito.spy;

import com.torodb.core.TableRefFactory;
import com.torodb.core.impl.TableRefFactoryImpl;
import com.torodb.core.transaction.metainf.FieldIndexOrdering;
import com.torodb.core.transaction.metainf.FieldType;
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
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import com.torodb.core.transaction.metainf.UnmergeableException;
import com.torodb.core.transaction.metainf.WrapperMutableMetaSnapshot;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.MockSettings;
import org.mockito.internal.creation.MockSettingsImpl;

/**
 *
 * Index merging:
 *
 * Definitions: - List of fields match: same size and at equal position in the list: none field
 * table ref that differ and none field names that differ and none field ordering that differ and no
 * index options that differ. - Sublist of fields for a table ref match: all fields have same table
 * ref and same size and at equal position in the sublist: none field names that differ and none
 * field ordering that differ. - Index match: indexes have equal names or, they have no index
 * options that differ and their lists of fields match - "old new", "old renmoved", "new", "removed"
 * and "old" adjectives: - "new" is used to refer to an index or field added with last change. -
 * "removed" is used to refer to an index removed with last change - "old new" and "old removed" are
 * used to refer to, respectively, the added or removed index of the change previous to the last
 * one. - "old" is used to refer to an index or field that was present before the last one - Field
 * complete an index match: a field alone or together with other fields in some combination is part
 * of a doc part index that match an index.
 *
 * Operations:
 *
 * 1. Create index 2. Remove index 3. Create field
 *
 * Merge combinations or operations:
 *
 * 1&1: Create an index in parallel but after to another 1&1.OK1: new index and old new index does
 * not match 1&1.KO1: new index and old new index match (Can not create same index twice)
 *
 * 1&2: Create index in parallel but after to remove another 1&2.OK1: no new index and old removed
 * index sublist of fields match for any table ref 1&2.KO1: any new index and old removed index
 * sublist of fields match for any table ref (Missing doc part index when a doc part index needed by
 * new index has been removed)
 *
 * 1&3: Create an index in parallel but after to create a field 1&3.OK1: old new field does not
 * complete a new index match 1&3.OK2: old new field hit an index match for new index and for
 * another old index 1&3.KO1: old new field hit an index match (Missing doc part index needed by a
 * index)
 *
 * 2&1: Remove an index in parallel but after to create an index 2&1.OK1: removed index and old new
 * index does not match 2&1.OK2: removed index is associated with old doc part index. Removed index
 * and old new index does not match 2&1.KO1: removed index and old new index does match
 *
 * 2&2: Remove an index in parallel but after to remove an index 2&2.OK1: removed index and old
 * removed index does not have the same names 2&2.OK2: removed index is associated with old doc part
 * index. Removed index and old removed index does not have the same names 2&2.OK3: removed index
 * and old removed index have the same names
 *
 * 2&3: Remove an index in parallel but after to create a field 2&3.OK1: old new field does not
 * complete an index match for removed index 2&3.OK2: removed index is associated with old doc part
 * index. old new field does not complete an index match for removed index 2&3.OK3: old new field
 * hit an index match for removed index and for another old index 2&3.KO1: old new field hit an
 * index match (Orphan doc part index that no index use)
 *
 * 3&1: Create field in parallel but after to create an index 3&1.OK1: new field does not complete
 * an index match with old new index 3&1.OK2: new field complete an index match with old new index
 * but also complete an index match with another old index 3&1.KO1: new field complete an index
 * match with old new index (Missing doc part index needed by an index)
 *
 * 3&2: Create field in parallel but after to remove an index 3&2.OK1: new field does not complete
 * an index match with old removed index 3&2.OK2: new field complete an index match with old removed
 * index but also complete an index match with another old index 3&2.KO1: new field complete an
 * index match with old removed index (Orphan doc part index needed by an index)
 *
 * 3&3: Create field in parallel but after to create a field 3&3.OK1: new field and old new field
 * does not complete an index match with an old index 3&3.KO1: new field and old new field complete
 * an index match with an old index
 *
 * Each combination should be repeated with: 1 index of 1 field 2 index of 2 fields on same table
 * ref 3 index of 4 fields with each 2 fields on a different table ref - In this combination there
 * could be two variant when, respectively, one or two index sublists of fields for each table ref
 * participate actively in the merge (this seem a bit paranoid test but we added it just in case).
 */
@SuppressWarnings({"unused", "rawtypes"})
public class SnapshotMergerForIndexTest {

  private static final TableRefFactory tableRefFactory = new TableRefFactoryImpl();
  private static ImmutableMetaSnapshot currentSnapshot;
  private ImmutableMetaSnapshot.Builder snapshotBuilder;

  private static final MockSettings SETTINGS = new MockSettingsImpl().defaultAnswer((t) -> {
    throw new AssertionError("Method " + t.getMethod() + " was not expected to be called");
  });

  public SnapshotMergerForIndexTest() {
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    currentSnapshot = new ImmutableMetaSnapshot.Builder()
        .put(new ImmutableMetaDatabase.Builder("dbName1", "dbId1")
            .put(new ImmutableMetaCollection.Builder("colName1", "colId1")
                .put(new ImmutableMetaDocPart.Builder(tableRefFactory.createRoot(), "docPartId1")
                    .put(new ImmutableMetaField("fieldName1", "fieldId1", FieldType.INTEGER))
                    .put(new ImmutableMetaScalar("scalarId1", FieldType.INTEGER))
                    .put(new ImmutableMetaIdentifiedDocPartIndex.Builder("idxId1", false)
                        .add(new ImmutableMetaDocPartIndexColumn(0, "fieldId1",
                            FieldIndexOrdering.ASC))
                    )
                )
                .put(new ImmutableMetaIndex.Builder("idxName1", false)
                    .add(new ImmutableMetaIndexField(0, tableRefFactory.createRoot(), "fieldName1",
                        FieldIndexOrdering.ASC))
                )
                .build()
            ).build()
        ).build();
  }

  @Before
  public void setup() {
    snapshotBuilder = spy(new ImmutableMetaSnapshot.Builder(currentSnapshot));
  }

  /**
   * Test a new index Case [1&1.OK1|1&2.OK1].1
   *
   * @throws Exception
   */
  @Test
  public void testNewIndex() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);

    new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
  }

  /**
   * Test a new index with multiple fields Case [1&1.OK1|1&2.OK1].2
   *
   * @throws Exception
   */
  @Test
  public void testNewIndexMultiField() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName4", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName5", FieldIndexOrdering.ASC);

    new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
  }

  /**
   * Test a new index with multiple fields in multiple doc parts Case [1&1.OK1|1&2.OK1].3
   *
   * @throws Exception
   */
  @Test
  public void testNewIndexMultiFieldInMultiDocParts() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName4", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName5", FieldIndexOrdering.ASC);

    new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
  }

  /**
   * Test an exception is thrown when a new index has same name as an old index Case 1&1.KO1.1
   *
   * @throws Exception
   */
  @Test
  public void testNewIndexWithSameNameAsOldIndexConflict() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", true)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.DESC);
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);

    try {
      new SnapshotMerger(currentModifiedSnapshot.immutableCopy(), changedSnapshot).merge();
      Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
    } catch (UnmergeableException ex) {

    }
  }

  /**
   * Test an exception is thrown when a new index match an old index Case 1&1.KO1.1a
   *
   * @throws Exception
   */
  @Test
  public void testNewIndexWhichMatchOldIndexConflict() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName3", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);

    try {
      new SnapshotMerger(currentModifiedSnapshot.immutableCopy(), changedSnapshot).merge();
      Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
    } catch (UnmergeableException ex) {

    }
  }

  /**
   * Test an exception is thrown when a new index with multiple fields match an old index Case
   * 1&1.KO1.2
   *
   * @throws Exception
   */
  @Test
  public void testNewIndexMultiFielWhichdMatchOldIndexConflict() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName4", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName5", FieldIndexOrdering.ASC);
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName3", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName3")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName3")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName4", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName3")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName5", FieldIndexOrdering.ASC);

    try {
      new SnapshotMerger(currentModifiedSnapshot.immutableCopy(), changedSnapshot).merge();
      Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
    } catch (UnmergeableException ex) {

    }
  }

  /**
   * Test an exception is thrown when a new index with multiple fields in multiple doc parts match
   * an old index Case 1&1.KO1.3
   *
   * @throws Exception
   */
  @Test
  public void testNewIndexMultiFieldInMultiDocPartsWhichMatchOldIndexConflict() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName4", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName5", FieldIndexOrdering.ASC);
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName4", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName5", FieldIndexOrdering.ASC);

    try {
      new SnapshotMerger(currentModifiedSnapshot.immutableCopy(), changedSnapshot).merge();
      Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
    } catch (UnmergeableException ex) {

    }
  }

  /**
   * Test an exception is thrown when a new index has same doc part index as an old remove index
   * Case 1&2.KO1.1
   *
   * @throws Exception
   */
  @Test
  public void testNewIndexWithOldIndexWithSameDocPartIndexRemovedConflict() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", true)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.DESC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName3", FieldIndexOrdering.DESC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId2");
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .removeMetaIndexByName("idxName2");
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .removeMetaDocPartIndexByIdentifier("idxId2");
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName3", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);

    try {
      new SnapshotMerger(currentModifiedSnapshot.immutableCopy(), changedSnapshot).merge();
      Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
    } catch (UnmergeableException ex) {

    }
  }

  /**
   * Test an exception is thrown when a new index with multiple fields has same doc part index as an
   * old remove index Case 1&2.KO1.2
   *
   * @throws Exception
   */
  @Test
  public void testNewIndexWithMultiFieldsWithOldIndexWithSameDocPartIndexRemovedConflict() throws
      Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName3", "fieldId3", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", true)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.DESC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.DESC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName4", FieldIndexOrdering.DESC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId2");
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .removeMetaIndexByName("idxName2");
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .removeMetaDocPartIndexByIdentifier("idxId2");
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName3", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName3")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);

    try {
      new SnapshotMerger(currentModifiedSnapshot.immutableCopy(), changedSnapshot).merge();
      Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
    } catch (UnmergeableException ex) {

    }
  }

  /**
   * Test an exception is thrown when a new index with multiple fields in multiple doc parts has
   * same doc part index as an old remove index Case 1&2.KO1.3
   *
   * @throws Exception
   */
  @Test
  public void testNewIndexWithMultiFieldsInMultiDocPartsWithOldIndexWithSameDocPartIndexRemovedConflict()
      throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName3", "fieldId3", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", true)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.DESC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.DESC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName3"),
            "fieldName6", FieldIndexOrdering.DESC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId2");
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .removeMetaIndexByName("idxName2");
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .removeMetaDocPartIndexByIdentifier("idxId2");
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName3", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName3")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName4", FieldIndexOrdering.DESC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName5", FieldIndexOrdering.DESC);

    try {
      new SnapshotMerger(currentModifiedSnapshot.immutableCopy(), changedSnapshot).merge();
      Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
    } catch (UnmergeableException ex) {

    }
  }

  /**
   * Test a new index and new doc part index Case 1&3.OK1.1
   *
   * @throws Exception
   */
  @Test
  public void testNewIndexAndNewDocPartIndex() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId2");

    new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
  }

  /**
   * Test a new index with multiple fields and new doc part index Case 1&3.OK1.2
   *
   * @throws Exception
   */
  @Test
  public void testNewIndexMultiFieldsAndNewDocPartIndex() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName3", "fieldId3", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName4", "fieldId4", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName5", "fieldId5", FieldType.STRING);
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName4", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName5", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId3", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId4", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId5", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId2");

    new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
  }

  /**
   * Test a new index with multiple fields in multiple doc parts and new doc part index Case
   * 1&3.OK1.3
   *
   * @throws Exception
   */
  @Test
  public void testNewIndexMultiFieldsInMultiDocPartsAndNewDocPartIndex() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName3", "fieldId3", FieldType.STRING);
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName4", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName5", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId3", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId2");

    new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
  }

  /**
   * Test a new index with multiple fields in multiple doc parts and new doc part indexes Case
   * 1&3.OK1.3b
   *
   * @throws Exception
   */
  @Test
  public void testNewIndexMultiFieldsInMultiDocPartsAndNewDocPartIndexes() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName3", "fieldId3", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaDocPart(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "docPartId2");
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createChild(tableRefFactory.createRoot(),
            "docPartName2"))
        .addMetaField("fieldName4", "fieldId4", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createChild(tableRefFactory.createRoot(),
            "docPartName2"))
        .addMetaField("fieldName5", "fieldId5", FieldType.STRING);
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName4", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName5", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId3", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId2");
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createChild(tableRefFactory.createRoot(),
            "docPartName2"))
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId4", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createChild(tableRefFactory.createRoot(),
            "docPartName2"))
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId5", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createChild(tableRefFactory.createRoot(),
            "docPartName2"))
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId3");

    new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
  }

  /**
   * Test a new index and old doc part index Case 1&3.OK2.1
   *
   * @throws Exception
   */
  @Test
  public void testNewIndexWithOldDocPartIndex() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName3"),
            "fieldName6", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId2");
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName3", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);

    new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
  }

  /**
   * Test a new index with multiple fields and old doc part index Case 1&3.OK2.2
   *
   * @throws Exception
   */
  @Test
  public void testNewIndexMultiFieldsAndWithOldDocPartIndex() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName3", "fieldId3", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName4", "fieldId4", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName5", "fieldId5", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName3", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName3")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName3")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName4", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName3")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName5", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName3")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName3"),
            "fieldName6", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId4", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId5", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId2");
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName4", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName5", FieldIndexOrdering.ASC);

    new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
  }

  /**
   * Test a new index with multiple fields and old doc part index and a new one Case 1&3.OK2.3
   *
   * @throws Exception
   */
  @Test
  public void testNewIndexMultiFieldsInMultiDocPartsWithOldDocPartIndexAndNewDocPartIndex() throws
      Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName3", "fieldId3", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaDocPart(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "docPartId2");
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createChild(tableRefFactory.createRoot(),
            "docPartName2"))
        .addMetaField("fieldName4", "fieldId4", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createChild(tableRefFactory.createRoot(),
            "docPartName2"))
        .addMetaField("fieldName5", "fieldId5", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName3", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName3")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName3")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName4", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName3")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName5", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName3")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName3"),
            "fieldName6", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId2");
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName4", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName5", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createChild(tableRefFactory.createRoot(),
            "docPartName2"))
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId4", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createChild(tableRefFactory.createRoot(),
            "docPartName2"))
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId5", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createChild(tableRefFactory.createRoot(),
            "docPartName2"))
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId3");

    new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
  }

  /**
   * Test a new index with multiple fields and old doc part indexes Case 1&3.OK2.3b
   *
   * @throws Exception
   */
  @Test
  public void testNewIndexMultiFieldsInMultiDocPartsWithOldDocPartIndexes() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName3", "fieldId3", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaDocPart(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "docPartId2");
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createChild(tableRefFactory.createRoot(),
            "docPartName2"))
        .addMetaField("fieldName4", "fieldId4", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createChild(tableRefFactory.createRoot(),
            "docPartName2"))
        .addMetaField("fieldName5", "fieldId5", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId2");
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createChild(tableRefFactory.createRoot(),
            "docPartName2"))
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId4", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createChild(tableRefFactory.createRoot(),
            "docPartName2"))
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId5", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createChild(tableRefFactory.createRoot(),
            "docPartName2"))
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId3");
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName4", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName5", FieldIndexOrdering.ASC);

    new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
  }

  /**
   * Test a new field and new doc part index Case 3&1.OK1.1
   *
   * @throws Exception
   */
  @Test
  public void testNewFieldAndNewDocPartIndex() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId2");

    new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
  }

  /**
   * Test a new fields and new doc part index with multiple columns Case 3&1.OK1.2
   *
   * @throws Exception
   */
  @Test
  public void testNewFieldsAndNewDocPartIndexMultiColumns() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName4", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName5", FieldIndexOrdering.ASC);
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName3", "fieldId3", FieldType.STRING);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName4", "fieldId4", FieldType.STRING);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName5", "fieldId5", FieldType.STRING);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId3", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId4", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId5", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId2");

    new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
  }

  /**
   * Test a new fields and new doc part index with multiple columns in multiple doc parts Case
   * 3&1.OK1.3
   *
   * @throws Exception
   */
  @Test
  public void testNewFieldsAndNewDocPartIndexMultiColumnsInMultiDocParts() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaDocPart(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "docPartId2");
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName4", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName5", FieldIndexOrdering.ASC);
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName3", "fieldId3", FieldType.STRING);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createChild(tableRefFactory.createRoot(),
            "docPartName2"))
        .addMetaField("fieldName4", "fieldId4", FieldType.STRING);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createChild(tableRefFactory.createRoot(),
            "docPartName2"))
        .addMetaField("fieldName5", "fieldId5", FieldType.STRING);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId3", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId2");
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createChild(tableRefFactory.createRoot(),
            "docPartName2"))
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId4", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createChild(tableRefFactory.createRoot(),
            "docPartName2"))
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId5", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createChild(tableRefFactory.createRoot(),
            "docPartName2"))
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId3");

    new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
  }

  /**
   * Test a new field and new doc part index with old removed index Case 3&2.OK1.1
   *
   * @throws Exception
   */
  @Test
  public void testNewFieldAndNewDocPartIndexWithOldRemovedIndex() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName3", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .removeMetaIndexByName("idxName3");
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId2");

    new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
  }

  /**
   * Test a new fields and new doc part index with multiple columns with old removed index Case
   * 3&2.OK1.2
   *
   * @throws Exception
   */
  @Test
  public void testNewFieldsAndNewDocPartIndexMultiColumnsWithOldRemovedIndex() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName3", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName4", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName3")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName5", FieldIndexOrdering.ASC);
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .removeMetaIndexByName("idxName3");
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName3", "fieldId3", FieldType.STRING);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId3", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId2");

    new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
  }

  /**
   * Test a new fields and new doc part index with multiple columns in multiple doc parts with old
   * removed index Case 3&2.OK1.3
   *
   * @throws Exception
   */
  @Test
  public void testNewFieldsAndNewDocPartIndexMultiColumnsInMultiDocPartsWithOldRemovedIndex() throws
      Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaDocPart(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "docPartId2");
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName4", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName5", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName3", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName6", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName3")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName7", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName3")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName8", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName3")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName9", FieldIndexOrdering.ASC);
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .removeMetaIndexByName("idxName2");
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName3", "fieldId3", FieldType.STRING);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createChild(tableRefFactory.createRoot(),
            "docPartName2"))
        .addMetaField("fieldName4", "fieldId4", FieldType.STRING);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createChild(tableRefFactory.createRoot(),
            "docPartName2"))
        .addMetaField("fieldName5", "fieldId5", FieldType.STRING);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId3", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId2");
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createChild(tableRefFactory.createRoot(),
            "docPartName2"))
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId4", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createChild(tableRefFactory.createRoot(),
            "docPartName2"))
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId5", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createChild(tableRefFactory.createRoot(),
            "docPartName2"))
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId3");

    new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
  }

  /**
   * Test remove an index Case [2&1.OK1|2&2.OK1|2&3.OK1].1
   *
   * @throws Exception
   */
  @Test
  public void testRemoveIndex() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .removeMetaIndexByName("idxName2");

    new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
  }

  /**
   * Test remove an index with multiple fields Case [2&1.OK1|2&2.OK1|2&3.OK1].2
   *
   * @throws Exception
   */
  @Test
  public void testRemoveIndexWithMuliFields() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName4", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName5", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName3", "fieldId3", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName4", "fieldId4", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName5", "fieldId5", FieldType.STRING);
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .removeMetaIndexByName("idxName2");

    new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
  }

  /**
   * Test remove an index with multiple fields in multiple doc parts Case
   * [2&1.OK1|2&2.OK1|2&3.OK1].3
   *
   * @throws Exception
   */
  @Test
  public void testRemoveIndexWithMuliFieldsInMultiDocParts() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaDocPart(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "docPartId2");
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName4", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName5", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName3", "fieldId3", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createChild(tableRefFactory.createRoot(),
            "docPartName2"))
        .addMetaField("fieldName4", "fieldId4", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createChild(tableRefFactory.createRoot(),
            "docPartName2"))
        .addMetaField("fieldName5", "fieldId5", FieldType.STRING);
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .removeMetaIndexByName("idxName2");

    new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
  }

  /**
   * Test remove an index and remove a doc part index Case [2&1.OK2|2&2.OK2|2&3.OK2].1
   *
   * @throws Exception
   */
  @Test
  public void testRemoveIndexAndRemoveDocPartIndex() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .removeMetaIndexByName("idxName1");
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .removeMetaDocPartIndexByIdentifier("idxId1");

    new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
  }

  /**
   * Test remove an index with multiple fields and remove a doc part index Case
   * [2&1.OK2|2&2.OK2|2&3.OK2].2
   *
   * @throws Exception
   */
  @Test
  public void testRemoveIndexWithMultiFieldsAndRemoveDocPartIndex() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName4", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName5", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName3", "fieldId3", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName4", "fieldId4", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName5", "fieldId5", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId4", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId5", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId2");
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .removeMetaIndexByName("idxName2");
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .removeMetaDocPartIndexByIdentifier("idxId2");

    new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
  }

  /**
   * Test remove an index with multiple fields in multiple doc parts and remove multiple doc part
   * indexes Case [2&1.OK2|2&2.OK2|2&3.OK2].3
   *
   * @throws Exception
   */
  @Test
  public void testRemoveIndexWithMultiFieldsInMultiDocPartsAndRemoveDocPartIndexes() throws
      Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaDocPart(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "docPartId2");
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName4", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName5", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName3", "fieldId3", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createChild(tableRefFactory.createRoot(),
            "docPartName2"))
        .addMetaField("fieldName4", "fieldId4", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createChild(tableRefFactory.createRoot(),
            "docPartName2"))
        .addMetaField("fieldName5", "fieldId5", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId2");
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createChild(tableRefFactory.createRoot(),
            "docPartName2"))
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId4", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createChild(tableRefFactory.createRoot(),
            "docPartName2"))
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId5", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createChild(tableRefFactory.createRoot(),
            "docPartName2"))
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId3");
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .removeMetaIndexByName("idxName2");
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .removeMetaDocPartIndexByIdentifier("idxId2");
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createChild(tableRefFactory.createRoot(),
            "docPartName2"))
        .removeMetaDocPartIndexByIdentifier("idxId3");

    new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
  }

  /**
   * Test remove an index that relates to a doc part index that also relate to another index Case
   * 2&1.OK1.1
   *
   * @throws Exception
   */
  @Test
  public void testRemoveIndexWithDocPartIndexIncludedByOtherIndex() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName1", FieldIndexOrdering.ASC);
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .removeMetaIndexByName("idxName2");

    new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
  }

  /**
   * Test remove an index with multiple fields that relates to a doc part index that also relate to
   * another index Case 2&1.OK1.2
   *
   * @throws Exception
   */
  @Test
  public void testRemoveIndexWithMultiFieldsWithDocPartIndexIncludedByOtherIndex() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName4", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName5", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName3", "fieldId3", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName4", "fieldId4", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName5", "fieldId5", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId4", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId5", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId2");
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName3", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName3")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName3")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName4", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName3")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName5", FieldIndexOrdering.ASC);
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .removeMetaIndexByName("idxName2");

    new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
  }

  /**
   * Test an exception is thrown when an index is removed and has same doc part index as an old
   * added index Case 2&1.KO1.1
   *
   * @throws Exception
   */
  @Test
  public void testRemoveIndexWithOldIndexWithSameDocPartIndexAddedConflict() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", true)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId2");
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName3", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .removeMetaIndexByName("idxName2");
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .removeMetaDocPartIndexByIdentifier("idxId2");

    try {
      new SnapshotMerger(currentModifiedSnapshot.immutableCopy(), changedSnapshot).merge();
      Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
    } catch (UnmergeableException ex) {

    }
  }

  /**
   * Test an exception is thrown when an index with multiple fields is removed and has same doc part
   * index as an old added index Case 2&1.KO1.2
   *
   * @throws Exception
   */
  @Test
  public void testRemoveIndexWithMultiFieldsWithOldIndexWithSameDocPartIndexAddedConflict() throws
      Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName3", "fieldId3", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", true)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName4", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId2");
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName3", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName3")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .removeMetaIndexByName("idxName2");
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .removeMetaDocPartIndexByIdentifier("idxId2");

    try {
      new SnapshotMerger(currentModifiedSnapshot.immutableCopy(), changedSnapshot).merge();
      Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
    } catch (UnmergeableException ex) {

    }
  }

  /**
   * Test an exception is thrown when an index with multiple fields in multiple doc parts is removed
   * and has same doc part index as an old added index Case 2&1.KO1.3
   *
   * @throws Exception
   */
  @Test
  public void testRemoveIndexWithMultiFieldsInMultiDocPartsWithOldIndexWithSameDocPartIndexAddedConflict()
      throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName3", "fieldId3", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", true)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName3"),
            "fieldName6", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId2");
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName3", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName3")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName3")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName4", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName3")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName5", FieldIndexOrdering.ASC);
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .removeMetaIndexByName("idxName2");
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .removeMetaDocPartIndexByIdentifier("idxId2");

    try {
      new SnapshotMerger(currentModifiedSnapshot.immutableCopy(), changedSnapshot).merge();
      Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
    } catch (UnmergeableException ex) {

    }
  }

  /**
   * Test when an index is removed and has same old removed index Case 2&2.OK3.1
   *
   * @throws Exception
   */
  @Test
  public void testRemoveIndexWithSameOldRemovedIndex() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", true)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId2");
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .removeMetaIndexByName("idxName2");
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .removeMetaDocPartIndexByIdentifier("idxId2");
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .removeMetaIndexByName("idxName2");
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .removeMetaDocPartIndexByIdentifier("idxId2");

    new SnapshotMerger(currentModifiedSnapshot.immutableCopy(), changedSnapshot).merge();
  }

  /**
   * Test when an index with multiple fields is removed and has same old removed index Case
   * 2&2.OK3.2
   *
   * @throws Exception
   */
  @Test
  public void testRemoveIndexWithMultiFieldsWithOldIndexWithSameDocPartIndexRemoved() throws
      Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName3", "fieldId3", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", true)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName4", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId2");
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .removeMetaIndexByName("idxName2");
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .removeMetaDocPartIndexByIdentifier("idxId2");
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .removeMetaIndexByName("idxName2");
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .removeMetaDocPartIndexByIdentifier("idxId2");

    new SnapshotMerger(currentModifiedSnapshot.immutableCopy(), changedSnapshot).merge();
  }

  /**
   * Test when an index with multiple fields in multiple doc parts is removed and has same old
   * removed index Case 2&2.OK3.3
   *
   * @throws Exception
   */
  @Test
  public void testRemoveIndexWithMultiFieldsInMultiDocPartsWithOldIndexWithSameDocPartIndexRemoved()
      throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName3", "fieldId3", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", true)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName3"),
            "fieldName6", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId2");
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .removeMetaIndexByName("idxName2");
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .removeMetaDocPartIndexByIdentifier("idxId2");
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .removeMetaIndexByName("idxName2");
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .removeMetaDocPartIndexByIdentifier("idxId2");

    new SnapshotMerger(currentModifiedSnapshot.immutableCopy(), changedSnapshot).merge();
  }

  /**
   * Test that an exception is thrown on new index with missing doc part index conflicts Case
   * 1&3.KO1.1
   *
   * @throws Exception
   */
  @Test
  public void testIndexWithOldMissingDocPartIndexConflict() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);

    try {
      new SnapshotMerger(currentModifiedSnapshot.immutableCopy(), changedSnapshot).merge();
      Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
    } catch (UnmergeableException ex) {

    }
  }

  /**
   * Test that an exception is thrown on new index with multiple fields with missing doc part index
   * conflicts Case 1&3.KO1.2
   *
   * @throws Exception
   */
  @Test
  public void testIndexWithMultiFieldsWithOldMissingDocPartIndexConflict() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName3", "fieldId3", FieldType.STRING);
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);

    try {
      new SnapshotMerger(currentModifiedSnapshot.immutableCopy(), changedSnapshot).merge();
      Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
    } catch (UnmergeableException ex) {

    }
  }

  /**
   * Test that an exception is thrown on new index with multiple fields in multiple doc parts with
   * missing doc part index conflicts Case 1&3.KO1.3
   *
   * @throws Exception
   */
  @Test
  public void testIndexWithMultiFieldsInMultiDocPartsWithOldMissingDocPartIndexConflict() throws
      Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName3", "fieldId3", FieldType.STRING);
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName4", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName5", FieldIndexOrdering.ASC);

    try {
      new SnapshotMerger(currentModifiedSnapshot.immutableCopy(), changedSnapshot).merge();
      Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
    } catch (UnmergeableException ex) {

    }
  }

  /**
   * Test that an exception is thrown on new field with missing doc part index conflicts Case
   * 3&1.KO1.1
   *
   * @throws Exception
   */
  @Test
  public void testIndexWithMissingDocPartIndexConflict() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);

    try {
      new SnapshotMerger(currentModifiedSnapshot.immutableCopy(), changedSnapshot).merge();
      Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
    } catch (UnmergeableException ex) {

    }
  }

  /**
   * Test that an exception is thrown on new field with missing doc part index with multiple fields
   * conflicts Case 3&1.KO1.2
   *
   * @throws Exception
   */
  @Test
  public void testIndexWithMultiFieldsWithMissingDocPartIndexConflict() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName3", "fieldId3", FieldType.STRING);

    try {
      new SnapshotMerger(currentModifiedSnapshot.immutableCopy(), changedSnapshot).merge();
      Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
    } catch (UnmergeableException ex) {

    }
  }

  /**
   * Test that an exception is thrown on new field with missing doc part index with multiple fields
   * in multiple doc parts conflicts Case 3&1.KO1.3
   *
   * @throws Exception
   */
  @Test
  public void testIndexWithMultiFieldsInMultiDocPartsWithMissingDocPartIndexConflict() throws
      Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaDocPart(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "docPartId2");
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName4", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName5", FieldIndexOrdering.ASC);
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName3", "fieldId3", FieldType.STRING);

    try {
      new SnapshotMerger(currentModifiedSnapshot.immutableCopy(), changedSnapshot).merge();
      Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
    } catch (UnmergeableException ex) {

    }
  }

  /**
   * Test that an exception is thrown on removed index with orphan doc part index conflicts Case
   * 2&3.KO1.1
   *
   * @throws Exception
   */
  @Test
  public void testIndexWithOldOrphanDocPartIndexConflict() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName1", "fieldId2", FieldType.BINARY);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId2");
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .removeMetaIndexByName("idxName1");
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .removeMetaDocPartIndexByIdentifier("idxId1");

    try {
      new SnapshotMerger(currentModifiedSnapshot.immutableCopy(), changedSnapshot).merge();
      Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
    } catch (UnmergeableException ex) {

    }
  }

  /**
   * Test that an exception is thrown on removed index with multiple fields with orphan doc part
   * index conflicts Case 2&3.KO1.2
   *
   * @throws Exception
   */
  @Test
  public void testIndexWithMultiFieldsWithOldOrphanDocPartIndexConflict() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId12", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName3", "fieldId13", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId12", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId13", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId2");
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.BINARY);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName3", "fieldId3", FieldType.BINARY);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId3");
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .removeMetaIndexByName("idxName2");
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .removeMetaDocPartIndexByIdentifier("idxId2");

    try {
      new SnapshotMerger(currentModifiedSnapshot.immutableCopy(), changedSnapshot).merge();
      Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
    } catch (UnmergeableException ex) {

    }
  }

  /**
   * Test that an exception is thrown on removed index with multiple fields in multiple doc parts
   * with orphan doc part index conflicts Case 2&3.KO1.3
   *
   * @throws Exception
   */
  @Test
  public void testIndexWithMultiFieldsInMultiDocPartsWithOldOrphanDocPartIndexConflict() throws
      Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId12", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName3", "fieldId13", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaDocPart(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "docPartId2");
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createChild(tableRefFactory.createRoot(),
            "docPartName2"))
        .addMetaField("fieldName4", "fieldId14", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createChild(tableRefFactory.createRoot(),
            "docPartName2"))
        .addMetaField("fieldName5", "fieldId15", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName4", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName5", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId12", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId13", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId2");
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.BINARY);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName3", "fieldId3", FieldType.BINARY);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId3");
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .removeMetaIndexByName("idxName2");
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .removeMetaDocPartIndexByIdentifier("idxId2");

    try {
      new SnapshotMerger(currentModifiedSnapshot.immutableCopy(), changedSnapshot).merge();
      Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
    } catch (UnmergeableException ex) {

    }
  }

  /**
   * Test that an exception is thrown on new doc part index with missing index conflicts Case
   * 3&2.KO1.1
   *
   * @throws Exception
   */
  @Test
  public void testIndexWithNewOrphanDocPartIndexConflict() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .removeMetaIndexByName("idxName1");
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .removeMetaDocPartIndexByIdentifier("idxId1");
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName1", "fieldId2", FieldType.BINARY);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId2");

    try {
      new SnapshotMerger(currentModifiedSnapshot.immutableCopy(), changedSnapshot).merge();
      Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
    } catch (UnmergeableException ex) {

    }
  }

  /**
   * Test that an exception is thrown on new doc part index with missing index with multiple fields
   * conflicts Case 3&2.KO1.2
   *
   * @throws Exception
   */
  @Test
  public void testIndexWithMultiFieldsWithNewOrphanDocPartIndexConflict() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId12", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName3", "fieldId13", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId12", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId13", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId2");
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .removeMetaIndexByName("idxName2");
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .removeMetaDocPartIndexByIdentifier("idxId2");
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.BINARY);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName3", "fieldId3", FieldType.BINARY);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId3", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId3");

    try {
      new SnapshotMerger(currentModifiedSnapshot.immutableCopy(), changedSnapshot).merge();
      Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
    } catch (UnmergeableException ex) {

    }
  }

  /**
   * Test that an exception is thrown on new doc part index with missing index with multiple fields
   * in multiple doc parts conflicts Case 3&2.KO1.2
   *
   * @throws Exception
   */
  @Test
  public void testIndexWithMultiFieldsInMultiDocPartsWithNewOrphanDocPartIndexConflict() throws
      Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId12", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName3", "fieldId13", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaDocPart(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "docPartId2");
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createChild(tableRefFactory.createRoot(),
            "docPartName2"))
        .addMetaField("fieldName4", "fieldId14", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createChild(tableRefFactory.createRoot(),
            "docPartName2"))
        .addMetaField("fieldName5", "fieldId15", FieldType.STRING);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName4", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName5", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId12", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId13", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId2");
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .removeMetaIndexByName("idxName2");
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .removeMetaDocPartIndexByIdentifier("idxId2");
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.BINARY);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName3", "fieldId3", FieldType.BINARY);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId3", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId3");

    try {
      new SnapshotMerger(currentModifiedSnapshot.immutableCopy(), changedSnapshot).merge();
      Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
    } catch (UnmergeableException ex) {

    }
  }

  /**
   * Test new field with new doc part index for existing index with multiple fields conflicts Case
   * 3&3.OK1.2
   *
   * @throws Exception
   */
  @Test
  public void testIndexWithMultiFieldsWithNewFieldAndNewDocPartIndex() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentModifiedSnapshot
        .immutableCopy());
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName3", "fieldId3", FieldType.STRING);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId3", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId2");

    new SnapshotMerger(currentModifiedSnapshot.immutableCopy(), changedSnapshot).merge();
  }

  /**
   * Test new field with new doc part index for existing index with multiple fields in multiple doc
   * parts Case 3&3.OK1.3
   *
   * @throws Exception
   */
  @Test
  public void testIndexWithMultiFieldsInMultiDocPartsWithNewFieldAndNewDocPartIndex() throws
      Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaDocPart(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "docPartId2");
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName4", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName5", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentModifiedSnapshot
        .immutableCopy());
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName3", "fieldId3", FieldType.STRING);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaDocPartIndex(false)
        .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .addMetaDocPartIndexColumn("fieldId3", FieldIndexOrdering.ASC);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .getAddedMutableMetaDocPartIndexes().iterator().next()
        .immutableCopy("idxId2");

    new SnapshotMerger(currentModifiedSnapshot.immutableCopy(), changedSnapshot).merge();
  }

  /**
   * Test that an exception is thrown on new field with missing doc part index for existing index
   * with multiple fields conflicts Case 3&3.KO1.2
   *
   * @throws Exception
   */
  @Test
  public void testIndexWithMultiFieldsWithNewFieldAndMissingDocPartIndexConflict() throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName3", "fieldId3", FieldType.STRING);

    try {
      new SnapshotMerger(currentModifiedSnapshot.immutableCopy(), changedSnapshot).merge();
      Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
    } catch (UnmergeableException ex) {

    }
  }

  /**
   * Test that an exception is thrown on new field with missing doc part index for existing index
   * with multiple fields in multiple doc parts conflicts Case 3&3.KO1.3
   *
   * @throws Exception
   */
  @Test
  public void testIndexWithMultiFieldsInMultiDocPartsWithNewFieldAndMissingDocPartIndexConflict()
      throws Exception {
    MutableMetaSnapshot currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaIndex("idxName2", false)
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName2", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createRoot(), "fieldName3", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .addMetaDocPart(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "docPartId2");
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName4", FieldIndexOrdering.ASC);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaIndexByName("idxName2")
        .addMetaIndexField(tableRefFactory.createChild(tableRefFactory.createRoot(), "docPartName2"),
            "fieldName5", FieldIndexOrdering.ASC);
    ImmutableMetaSnapshot currentSnapshot = currentModifiedSnapshot.immutableCopy();
    currentModifiedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    currentModifiedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName2", "fieldId2", FieldType.STRING);
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(currentSnapshot);
    changedSnapshot
        .getMetaDatabaseByName("dbName1")
        .getMetaCollectionByName("colName1")
        .getMetaDocPartByTableRef(tableRefFactory.createRoot())
        .addMetaField("fieldName3", "fieldId3", FieldType.STRING);

    try {
      new SnapshotMerger(currentModifiedSnapshot.immutableCopy(), changedSnapshot).merge();
      Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
    } catch (UnmergeableException ex) {

    }
  }
}
