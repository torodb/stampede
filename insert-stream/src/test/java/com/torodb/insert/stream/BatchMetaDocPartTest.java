package com.torodb.insert.stream;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.Collections;
import java.util.function.Consumer;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.torodb.core.TableRefFactory;
import com.torodb.core.impl.TableRefFactoryImpl;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.WrapperMutableMetaDocPart;

/**
 *
 * @author gortiz
 */
public class BatchMetaDocPartTest {

    private final TableRefFactory tableRefFactory = new TableRefFactoryImpl();
    private BatchMetaDocPart docPart;
    private WrapperMutableMetaDocPart delegate;
    private Consumer<BatchMetaDocPart> testChangeConsumer;
    private Consumer<WrapperMutableMetaDocPart> delegateChangeConsumer;

    public BatchMetaDocPartTest() {
    }

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        testChangeConsumer = mock(Consumer.class);
        delegateChangeConsumer = mock(Consumer.class);

        delegate = new WrapperMutableMetaDocPart(
                new ImmutableMetaDocPart(tableRefFactory.createRoot(), "docPartId", Collections.emptyMap()),
                delegateChangeConsumer
        );
        docPart = new BatchMetaDocPart(delegate, testChangeConsumer, true);
    }

    @Test
    public void testNewBatch() {
        //PRECONDITIONS ON THE INITIAL STATE
        assertTrue(Iterables.isEmpty(docPart.getOnBatchModifiedMetaFields()));

        docPart.addMetaField("aFieldName", "aFieldId", FieldType.TIME);
        assertFalse("addMetaField is not working as expected", Iterables.isEmpty(docPart.getOnBatchModifiedMetaFields()));

        docPart.setCreatedOnCurrentBatch(true);

        //METHOD TO TEST CALL
        docPart.newBatch();

        //POST CONDITIONS
        assertFalse("newBatch should set isCreatedOnCurrentBatch to false",
                docPart.isCreatedOnCurrentBatch());
        assertTrue("newBatch did not clear onBatchModifiedMetaFields",
                Iterables.isEmpty(docPart.getOnBatchModifiedMetaFields()));
    }

    @Test
    public void testCreatedOnCurrentBatch() {
        docPart.setCreatedOnCurrentBatch(true);
        assertTrue(docPart.isCreatedOnCurrentBatch());

        docPart.setCreatedOnCurrentBatch(false);
        assertFalse(docPart.isCreatedOnCurrentBatch());
    }

    @Test
    public void testAddMetaField() {
        String fieldName = "aFieldName";
        String fieldId = "aFieldID";
        FieldType fieldType = FieldType.INTEGER;

        assertNull(delegate.getMetaFieldByIdentifier(fieldId));
        assertNull(delegate.getMetaFieldByNameAndType(fieldName, fieldType));

        assertNull(docPart.getMetaFieldByIdentifier(fieldId));
        assertNull(docPart.getMetaFieldByNameAndType(fieldName, fieldType));


        docPart.addMetaField(fieldName, fieldId, fieldType);

        
        assertNotNull(delegate.getMetaFieldByIdentifier(fieldId));
        assertNotNull(delegate.getMetaFieldByNameAndType(fieldName, fieldType));

        assertNotNull(docPart.getMetaFieldByIdentifier(fieldId));
        assertNotNull(docPart.getMetaFieldByNameAndType(fieldName, fieldType));

        assertFalse(Iterables.isEmpty(docPart.getAddedMetaFields()));
        assertFalse(Iterables.isEmpty(delegate.getAddedMetaFields()));
        assertFalse(Iterables.isEmpty(docPart.getOnBatchModifiedMetaFields()));

        verify(testChangeConsumer).accept(docPart);
        verifyNoMoreInteractions(testChangeConsumer);
    }
}
