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
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.ImmutableMetaField;
import com.torodb.core.transaction.metainf.ImmutableMetaScalar;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import com.torodb.core.transaction.metainf.UnmergeableException;
import com.torodb.core.transaction.metainf.WrapperMutableMetaSnapshot;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.MockSettings;
import org.mockito.internal.creation.MockSettingsImpl;

import java.util.Collections;

/**
 *
 * @author gortiz
 */
@SuppressWarnings({"unused", "rawtypes"})
public class SnapshotMergerTest {

  private static final TableRefFactory tableRefFactory = new TableRefFactoryImpl();
  private static ImmutableMetaSnapshot currentSnapshot;
  private ImmutableMetaSnapshot.Builder snapshotBuilder;

  private static final MockSettings SETTINGS = new MockSettingsImpl().defaultAnswer((t) -> {
    throw new AssertionError("Method " + t.getMethod() + " was not expected to be called");
  });

  public SnapshotMergerTest() {
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    currentSnapshot = new ImmutableMetaSnapshot.Builder()
        .put(new ImmutableMetaDatabase.Builder("dbName1", "dbId1")
            .put(new ImmutableMetaCollection.Builder("colName1", "colId1")
                .put(new ImmutableMetaDocPart.Builder(tableRefFactory.createRoot(), "docPartId1")
                    .put(new ImmutableMetaField("fieldName1", "fieldId1", FieldType.INTEGER))
                    .put(new ImmutableMetaScalar("scalarId1", FieldType.INTEGER))
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
   * Fails if the checker do not allow idempotency
   *
   * @throws Exception
   */
  @Test
  public void testIdempotency() throws Exception {
    SnapshotMerger merger;

    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot(
        Collections.emptyMap()));
    MutableMetaDocPart metaDocPart = changedSnapshot.addMetaDatabase("dbName1", "dbId1")
        .addMetaCollection("colName1", "colId1")
        .addMetaDocPart(tableRefFactory.createRoot(), "docPartId1");
    metaDocPart.addMetaField("fieldName1", "fieldId1", FieldType.INTEGER);
    metaDocPart.addMetaScalar("scalarId1", FieldType.INTEGER);

    merger = new SnapshotMerger(currentSnapshot, changedSnapshot);
    merger.merge();
  }

  /**
   * Test that an exception is thrown on database name conflicts
   *
   * @throws Exception
   */
  @Test
  public void testDatabaseNameConflict() throws Exception {
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot(
        Collections.emptyMap()));
    changedSnapshot.addMetaDatabase("dbName2", "dbId1");

    try {
      new SnapshotMerger(currentSnapshot, changedSnapshot)
          .merge();
      Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
    } catch (UnmergeableException ex) {

    }
  }

  /**
   * Test that an exception is thrown on database id conflicts
   *
   * @throws Exception
   */
  @Test
  public void testDatabaseIdConflict() throws Exception {
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot(
        Collections.emptyMap()));
    changedSnapshot.addMetaDatabase("dbName1", "dbId2");

    try {
      new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
      Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
    } catch (UnmergeableException ex) {

    }
  }

  /**
   * Test that an exception is thrown on collection name conflicts
   *
   * @throws Exception
   */
  @Test
  public void testCollectionNameConflict() throws Exception {
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot(
        Collections.emptyMap()));
    changedSnapshot.addMetaDatabase("dbName1", "dbId1")
        .addMetaCollection("colName2", "colId1");

    try {
      new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
      Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
    } catch (UnmergeableException ex) {

    }
  }

  /**
   * Test that an exception is thrown on collection id conflicts
   *
   * @throws Exception
   */
  @Test
  public void testCollectionIdConflict() throws Exception {
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot(
        Collections.emptyMap()));
    changedSnapshot.addMetaDatabase("dbName1", "dbId1")
        .addMetaCollection("colName1", "colId2");

    try {
      new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
      Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
    } catch (UnmergeableException ex) {

    }
  }

  /**
   * Test that an exception is thrown on doc part table ref conflicts
   *
   * @throws Exception
   */
  @Test
  public void testDocPartRefConflict() throws Exception {
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot(
        Collections.emptyMap()));
    changedSnapshot.addMetaDatabase("dbName1", "dbId1")
        .addMetaCollection("colName1", "colId1")
        .addMetaDocPart(tableRefFactory.createChild(tableRefFactory.createRoot(), "anotherTRef"),
            "docPartId1");

    try {
      new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
      Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
    } catch (UnmergeableException ex) {

    }
  }

  /**
   * Test that an exception is thrown on doc part id conflicts
   *
   * @throws Exception
   */
  @Test
  public void testDocPartIdConflict() throws Exception {
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot(
        Collections.emptyMap()));
    changedSnapshot.addMetaDatabase("dbName1", "dbId1")
        .addMetaCollection("colName1", "colId1")
        .addMetaDocPart(tableRefFactory.createRoot(), "docPartId2");

    try {
      new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
      Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
    } catch (UnmergeableException ex) {

    }
  }

  /**
   * Test that an exception is thrown on field name and type conflicts
   *
   * @throws Exception
   */
  @Test
  public void testFieldNameAndTypeConflict() throws Exception {
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot(
        Collections.emptyMap()));
    changedSnapshot.addMetaDatabase("dbName1", "dbId1")
        .addMetaCollection("colName1", "colId1")
        .addMetaDocPart(tableRefFactory.createRoot(), "docPartId1")
        .addMetaField("fieldName1", "fieldId1", FieldType.TIME);

    try {
      new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
      Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
    } catch (UnmergeableException ex) {

    }

    changedSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot(Collections
        .emptyMap()));
    changedSnapshot.addMetaDatabase("dbName1", "dbId1")
        .addMetaCollection("colName1", "colId1")
        .addMetaDocPart(tableRefFactory.createRoot(), "docPartId1")
        .addMetaField("fieldName2", "fieldId1", FieldType.INTEGER);

    try {
      new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
      Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
    } catch (UnmergeableException ex) {

    }
  }

  /**
   * Test that an exception is thrown on field id conflicts
   *
   * @throws Exception
   */
  @Test
  public void testFieldIdConflict() throws Exception {
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot(
        Collections.emptyMap()));
    changedSnapshot.addMetaDatabase("dbName1", "dbId1")
        .addMetaCollection("colName1", "colId1")
        .addMetaDocPart(tableRefFactory.createRoot(), "docPartId1")
        .addMetaField("fieldName1", "fieldId2", FieldType.INTEGER);

    try {
      new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
      Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
    } catch (UnmergeableException ex) {

    }
  }

  /**
   * Test that no exception is thrown when creating a new field that shares type but not name with a
   * previous one.
   *
   * @throws Exception
   */
  @Test
  public void testField_differentName() throws Exception {
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot(
        Collections.emptyMap()));
    changedSnapshot.addMetaDatabase("dbName1", "dbId1")
        .addMetaCollection("colName1", "colId1")
        .addMetaDocPart(tableRefFactory.createRoot(), "docPartId1")
        .addMetaField("fieldName2", "fieldId4", FieldType.INTEGER);

    new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
  }

  /**
   * Test that no exception is thrown when creating a new field that shares name but not type with a
   * previous one.
   *
   * @throws Exception
   */
  @Test
  public void testField_differentType() throws Exception {
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot(
        Collections.emptyMap()));
    changedSnapshot.addMetaDatabase("dbName1", "dbId1")
        .addMetaCollection("colName1", "colId1")
        .addMetaDocPart(tableRefFactory.createRoot(), "docPartId1")
        .addMetaField("fieldName1", "fieldId4", FieldType.TIME);

    new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
  }

  /**
   * Test that no exception is thrown when creating a new field with different id, name and type
   *
   * @throws Exception
   */
  @Test
  public void testField_different() throws Exception {
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot(
        Collections.emptyMap()));
    changedSnapshot.addMetaDatabase("dbName1", "dbId1")
        .addMetaCollection("colName1", "colId1")
        .addMetaDocPart(tableRefFactory.createRoot(), "docPartId1")
        .addMetaField("fieldName10", "fieldId20", FieldType.CHILD);

    new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
  }

  /**
   * Test that an exception is thrown on scalar id conflicts
   *
   * @throws Exception
   */
  @Test
  public void testScalarIdConflict() throws Exception {
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot(
        Collections.emptyMap()));
    changedSnapshot.addMetaDatabase("dbName1", "dbId1")
        .addMetaCollection("colName1", "colId1")
        .addMetaDocPart(tableRefFactory.createRoot(), "docPartId1")
        .addMetaScalar("scalarId1", FieldType.LONG);

    try {
      new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
      Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
    } catch (UnmergeableException ex) {

    }
  }

  /**
   * Test that an exception is thrown on scalar type conflicts
   *
   * @throws Exception
   */
  @Test
  public void testScalarTypeConflict() throws Exception {
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot(
        Collections.emptyMap()));
    changedSnapshot.addMetaDatabase("dbName1", "dbId1")
        .addMetaCollection("colName1", "colId1")
        .addMetaDocPart(tableRefFactory.createRoot(), "docPartId1")
        .addMetaScalar("fieldId2", FieldType.INTEGER);

    try {
      new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
      Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
    } catch (UnmergeableException ex) {

    }
  }

  /**
   * Test that no exception is thrown when creating a new scalar with different id and type
   *
   * @throws Exception
   */
  @Test
  public void testScalar_different() throws Exception {
    MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot(
        Collections.emptyMap()));
    changedSnapshot.addMetaDatabase("dbName1", "dbId1")
        .addMetaCollection("colName1", "colId1")
        .addMetaDocPart(tableRefFactory.createRoot(), "docPartId1")
        .addMetaScalar("fieldId20", FieldType.LONG);

    new SnapshotMerger(currentSnapshot, changedSnapshot).merge();
  }
}
