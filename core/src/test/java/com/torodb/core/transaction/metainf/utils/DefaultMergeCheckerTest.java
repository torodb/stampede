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
package com.torodb.core.transaction.metainf.utils;

import java.util.Collections;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.torodb.core.TableRefFactory;
import com.torodb.core.impl.TableRefFactoryImpl;
import com.torodb.core.impl.TableRefImpl;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.ImmutableMetaField;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import com.torodb.core.transaction.metainf.UnmergeableException;
import com.torodb.core.transaction.metainf.WrapperMutableMetaSnapshot;

/**
 *
 * @author gortiz
 */
public class DefaultMergeCheckerTest {

    private static final TableRefFactory tableRefFactory = new TableRefFactoryImpl();
    private static ImmutableMetaSnapshot currentSnapshot;

    public DefaultMergeCheckerTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        currentSnapshot = new ImmutableMetaSnapshot.Builder()
                .add(new ImmutableMetaDatabase.Builder("dbName1", "dbId1")
                        .add(new ImmutableMetaCollection.Builder("colName1", "colId1")
                                .add(new ImmutableMetaDocPart.Builder(tableRefFactory.createRoot(), "docPartId1")
                                        .add(new ImmutableMetaField("fieldName1", "fieldId1", FieldType.INTEGER))
                                ).build()
                        ).build()
                ).build();
    }

    /**
     * Fails if the checker do not allow idempotency
     * @throws Exception
     */
    @Test
    public void testIdempotency() throws Exception {
        MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot(Collections.emptyMap()));
        changedSnapshot.addMetaDatabase("dbName1", "dbId1")
                .addMetaCollection("colName1", "colId1")
                .addMetaDocPart(tableRefFactory.createRoot(), "docPartId1")
                .addMetaField("fieldName1", "fieldId1", FieldType.INTEGER);

        DefaultMergeChecker.checkMerge(currentSnapshot, changedSnapshot);
    }

    /**
     * Test that an exception is thrown on database name conflicts
     * @throws Exception
     */
    @Test
    public void testDatabaseNameConflict() throws Exception {
        MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot(Collections.emptyMap()));
        changedSnapshot.addMetaDatabase("dbName2", "dbId1");

        try {
            DefaultMergeChecker.checkMerge(currentSnapshot, changedSnapshot);
            Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
        } catch (UnmergeableException ex) {
            
        }
    }

    /**
     * Test that an exception is thrown on database id conflicts
     * @throws Exception
     */
    @Test
    public void testDatabaseIdConflict() throws Exception {
        MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot(Collections.emptyMap()));
        changedSnapshot.addMetaDatabase("dbName1", "dbId2");

        try {
            DefaultMergeChecker.checkMerge(currentSnapshot, changedSnapshot);
            Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
        } catch (UnmergeableException ex) {

        }
    }

    /**
     * Test that an exception is thrown on collection name conflicts
     * @throws Exception
     */
    @Test
    public void testCollectionNameConflict() throws Exception {
        MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot(Collections.emptyMap()));
        changedSnapshot.addMetaDatabase("dbName1", "dbId1")
                .addMetaCollection("colName2", "colId1");

        try {
            DefaultMergeChecker.checkMerge(currentSnapshot, changedSnapshot);
            Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
        } catch (UnmergeableException ex) {

        }
    }

    /**
     * Test that an exception is thrown on collection id conflicts
     * @throws Exception
     */
    @Test
    public void testCollectionIdConflict() throws Exception {
        MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot(Collections.emptyMap()));
        changedSnapshot.addMetaDatabase("dbName1", "dbId1")
                .addMetaCollection("colName1", "colId2");

        try {
            DefaultMergeChecker.checkMerge(currentSnapshot, changedSnapshot);
            Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
        } catch (UnmergeableException ex) {

        }
    }

    /**
     * Test that an exception is thrown on doc part table ref conflicts
     * @throws Exception
     */
    @Test
    public void testDocPartRefConflict() throws Exception {
        MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot(Collections.emptyMap()));
        changedSnapshot.addMetaDatabase("dbName1", "dbId1")
                .addMetaCollection("colName1", "colId1")
                .addMetaDocPart(tableRefFactory.createChild(tableRefFactory.createRoot(), "anotherTRef"), "docPartId1");

        try {
            DefaultMergeChecker.checkMerge(currentSnapshot, changedSnapshot);
            Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
        } catch (UnmergeableException ex) {

        }
    }

    /**
     * Test that an exception is thrown on doc part id conflicts
     * @throws Exception
     */
    @Test
    public void testDocPartIdConflict() throws Exception {
        MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot(Collections.emptyMap()));
        changedSnapshot.addMetaDatabase("dbName1", "dbId1")
                .addMetaCollection("colName1", "colId1")
                .addMetaDocPart(tableRefFactory.createRoot(), "docPartId2");

        try {
            DefaultMergeChecker.checkMerge(currentSnapshot, changedSnapshot);
            Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
        } catch (UnmergeableException ex) {

        }
    }

    /**
     * Test that an exception is thrown on field name and type conflicts
     * @throws Exception
     */
    @Test
    public void testFieldNameAndTypeConflict() throws Exception {
        MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot(Collections.emptyMap()));
        changedSnapshot.addMetaDatabase("dbName1", "dbId1")
                .addMetaCollection("colName1", "colId1")
                .addMetaDocPart(tableRefFactory.createRoot(), "docPartId1")
                .addMetaField("fieldName1", "fieldId1", FieldType.TIME);

        try {
            DefaultMergeChecker.checkMerge(currentSnapshot, changedSnapshot);
            Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
        } catch (UnmergeableException ex) {

        }

        changedSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot(Collections.emptyMap()));
        changedSnapshot.addMetaDatabase("dbName1", "dbId1")
                .addMetaCollection("colName1", "colId1")
                .addMetaDocPart(tableRefFactory.createRoot(), "docPartId1")
                .addMetaField("fieldName2", "fieldId1", FieldType.INTEGER);

        try {
            DefaultMergeChecker.checkMerge(currentSnapshot, changedSnapshot);
            Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
        } catch (UnmergeableException ex) {

        }
    }

    /**
     * Test that an exception is thrown on field id conflicts
     * @throws Exception
     */
    @Test
    public void testFieldIdConflict() throws Exception {
        MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot(Collections.emptyMap()));
        changedSnapshot.addMetaDatabase("dbName1", "dbId1")
                .addMetaCollection("colName1", "colId1")
                .addMetaDocPart(tableRefFactory.createRoot(), "docPartId1")
                .addMetaField("fieldName1", "fieldId2", FieldType.INTEGER);

        try {
            DefaultMergeChecker.checkMerge(currentSnapshot, changedSnapshot);
            Assert.fail("A " + UnmergeableException.class.getSimpleName() + " was expected to be thrown");
        } catch (UnmergeableException ex) {

        }
    }


    /**
     * Test that no exception is thrown when creating a new field that shares type but not name
     * with a previous one.
     * @throws Exception
     */
    @Test
    public void testField_differentName() throws Exception {
        MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot(Collections.emptyMap()));
        changedSnapshot.addMetaDatabase("dbName1", "dbId1")
                .addMetaCollection("colName1", "colId1")
                .addMetaDocPart(tableRefFactory.createRoot(), "docPartId1")
                .addMetaField("fieldName2", "fieldId4", FieldType.INTEGER);

        DefaultMergeChecker.checkMerge(currentSnapshot, changedSnapshot);
    }


    /**
     * Test that no exception is thrown when creating a new field that shares name but not type
     * with a previous one.
     * @throws Exception
     */
    @Test
    public void testField_differentType() throws Exception {
        MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot(Collections.emptyMap()));
        changedSnapshot.addMetaDatabase("dbName1", "dbId1")
                .addMetaCollection("colName1", "colId1")
                .addMetaDocPart(tableRefFactory.createRoot(), "docPartId1")
                .addMetaField("fieldName1", "fieldId4", FieldType.TIME);

        DefaultMergeChecker.checkMerge(currentSnapshot, changedSnapshot);
    }

    /**
     * Test that no exception is thrown when creating a new field with different id, name and type
     * @throws Exception
     */
    @Test
    public void testField_different() throws Exception {
        MutableMetaSnapshot changedSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot(Collections.emptyMap()));
        changedSnapshot.addMetaDatabase("dbName1", "dbId1")
                .addMetaCollection("colName1", "colId1")
                .addMetaDocPart(tableRefFactory.createRoot(), "docPartId1")
                .addMetaField("fieldName10", "fieldId20", FieldType.CHILD);

        DefaultMergeChecker.checkMerge(currentSnapshot, changedSnapshot);
    }
}