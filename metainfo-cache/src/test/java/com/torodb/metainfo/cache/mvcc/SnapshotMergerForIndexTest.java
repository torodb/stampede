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

import static org.mockito.Mockito.spy;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.MockSettings;
import org.mockito.internal.creation.MockSettingsImpl;

import com.torodb.core.TableRefFactory;
import com.torodb.core.impl.TableRefFactoryImpl;
import com.torodb.core.transaction.metainf.FieldIndexOrdering;
import com.torodb.core.transaction.metainf.FieldType;
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
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import com.torodb.core.transaction.metainf.UnmergeableException;
import com.torodb.core.transaction.metainf.WrapperMutableMetaSnapshot;

/**
 *
 * @author gortiz
 */
@SuppressWarnings({ "unused", "rawtypes" })
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
                                        .put(new ImmutableMetaDocPartIndex.Builder("idxId1", false)
                                                .add(new ImmutableMetaDocPartIndexColumn(0, "fieldId1", FieldIndexOrdering.ASC))
                                        )
                                )
                                .put(new ImmutableMetaIndex.Builder("idxName1", false)
                                        .add(new ImmutableMetaIndexField(0, tableRefFactory.createRoot(), "fieldName1", FieldIndexOrdering.ASC))
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
     * Test a new index and new doc part index
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
     * Test a new index and new doc part index
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
            .addMetaDocPartIndex("idxId2", false)
            .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);

        new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
    }

    /**
     * Test a new index and new doc part index
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
            .getMetaDocPartByTableRef(tableRefFactory.createRoot())
            .addMetaDocPartIndex("idxId2", false)
            .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);
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
     * Test a new field and new doc part index
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
            .addMetaDocPartIndex("idxId2", false)
            .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);

        new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
    }

    /**
     * Test remove an index and remove a doc part index
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
     * Test remove an index
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
     * Test remove an index that relates to a doc part index that also relate to another index
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
     * Test that an exception is thrown on new index with missing doc part index conflicts
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
     * Test that an exception is thrown on new doc part index with missing index conflicts
     * @throws Exception
     */
    @Test
    public void testIndexWithNewMissingDocPartIndexConflict() throws Exception {
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
            .addMetaDocPartIndex("idxId2", false)
            .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);

        try {
            new SnapshotMerger(currentModifiedSnapshot.immutableCopy(), changedSnapshot).merge();
            Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
        } catch (UnmergeableException ex) {

        }
    }

    /**
     * Test that an exception is thrown on new field with missing doc part index conflicts
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
     * Test that an exception is thrown on removed index with orphan doc part index conflicts
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
            .addMetaDocPartIndex("idxId2", false)
            .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);
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
     * Test that an exception is thrown on new doc part index with missing index conflicts
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
            .addMetaDocPartIndex("idxId2", false)
            .addMetaDocPartIndexColumn("fieldId2", FieldIndexOrdering.ASC);

        try {
            new SnapshotMerger(currentModifiedSnapshot.immutableCopy(), changedSnapshot).merge();
            Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
        } catch (UnmergeableException ex) {

        }
    }
}
