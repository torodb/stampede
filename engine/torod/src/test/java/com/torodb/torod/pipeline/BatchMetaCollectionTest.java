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

package com.torodb.torod.pipeline;

import static org.junit.Assert.*;
import static org.mockito.Mockito.verify;

import com.google.common.collect.Iterables;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.impl.TableRefFactoryImpl;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.WrapperMutableMetaCollection;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 *
 * @author gortiz
 */
public class BatchMetaCollectionTest {

  private final TableRefFactory tableRefFactory = new TableRefFactoryImpl();
  private BatchMetaCollection collection;
  private WrapperMutableMetaCollection delegate;

  public BatchMetaCollectionTest() {
  }

  @Before
  public void setUp() {
    ImmutableMetaCollection immutableCollection = new ImmutableMetaCollection.Builder("colName",
        "colId")
        .put(new ImmutableMetaDocPart(tableRefFactory.createRoot(), "docPartName"))
        .build();
    delegate = Mockito.spy(new WrapperMutableMetaCollection(immutableCollection, (o) -> {
    }));

    collection = new BatchMetaCollection(delegate);
  }

  @Test
  public void testConstructor() {
    assertTrue("The constructor do not copy doc parts contained by the delegate",
        collection.streamContainedMetaDocParts()
            .findAny()
            .isPresent()
    );
    assertTrue("There is at least one doc part that is marked as created on the current branch",
        collection.streamContainedMetaDocParts()
            .noneMatch((docPart) -> docPart.isCreatedOnCurrentBatch())
    );
  }

  @Test
  public void testStreamContainedMetaDocParts() {
  }

  @Test
  public void testNewBatch() {
    TableRef tableRef = tableRefFactory.createChild(tableRefFactory.createRoot(), "aPath");
    String tableId = "aTableId";

    BatchMetaDocPart newDocPart = collection.addMetaDocPart(tableRef, tableId);

    assertTrue("addMetaDocPart is not working properly", newDocPart.isCreatedOnCurrentBatch());
    assertFalse(Iterables.isEmpty(collection.getOnBatchModifiedMetaDocParts()));

    collection.newBatch();

    assertFalse(
        "A doc part created on the previous batch still thinks it is created on the next batch",
        newDocPart.isCreatedOnCurrentBatch());
    assertTrue(Iterables.isEmpty(collection.getOnBatchModifiedMetaDocParts()));
  }

  @Test
  public void testAddMetaDocPart() {
    TableRef tableRef = tableRefFactory.createChild(tableRefFactory.createRoot(), "aPath");
    String tableId = "aTableId";

    BatchMetaDocPart newDocPart = collection.addMetaDocPart(tableRef, tableId);

    assertEquals(newDocPart, collection.getMetaDocPartByTableRef(tableRef));
    assertNotNull(newDocPart);

    verify(delegate).addMetaDocPart(tableRef, tableId);
  }

  @Test
  public void testGetName() {
    assertEquals(collection.getName(), delegate.getName());
  }

  @Test
  public void testGetIdentifier() {
    assertEquals(collection.getIdentifier(), delegate.getIdentifier());
  }

}
