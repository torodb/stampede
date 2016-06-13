package com.torodb.insert.stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Iterables;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.impl.TableRefFactoryImpl;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.WrapperMutableMetaCollection;

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
        ImmutableMetaCollection immutableCollection = new ImmutableMetaCollection.Builder("colName", "colId")
                .add(new ImmutableMetaDocPart(tableRefFactory.createRoot(), "docPartName", Collections.emptyMap()))
                .build();
        delegate = Mockito.spy(new WrapperMutableMetaCollection(immutableCollection, (o) -> {}));

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

        assertFalse("A doc part created on the previous batch still thinks it is created on the next batch",
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
