package com.torodb.insert.stream;

import com.google.common.collect.Iterables;
import com.torodb.core.d2r.CollectionData;
import com.torodb.core.d2r.DocPartData;
import com.torodb.core.dsl.backend.BackendConnectionJob;
import com.torodb.core.dsl.backend.BackendConnectionJobFactory;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.MetaDatabase;
import java.util.Collections;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

/**
 *
 * @author gortiz
 */
public class DefaultToBackendFunctionTest {

    private BackendConnectionJobFactory factory;
    private ImmutableMetaCollection collection = new ImmutableMetaCollection("aColName", "aColId", Collections.emptyList());
    private MetaDatabase database = new ImmutableMetaDatabase("aDb", "aId", Collections.singletonList(collection));
    private DefaultToBackendFunction fun;

    public DefaultToBackendFunctionTest() {
    }

    @Before
    public void setUp() {
        factory = mock(BackendConnectionJobFactory.class);
        fun = new DefaultToBackendFunction(factory, database, collection);
    }

    @Test
    public void testApply() {
        CollectionData collectionData = mock(CollectionData.class);


        //when
//        fun.apply(collectionData);
    }

    @Test
    public void testApplyEmpty() {
        CollectionData collectionData = mock(CollectionData.class);
        given(collectionData.iterator())
                .willReturn(Collections.<DocPartData>emptyList().iterator());

        //when
        Iterable<BackendConnectionJob> resultIterable = fun.apply(collectionData);

        //then
        assertTrue("An empty iterator was expected", Iterables.isEmpty(resultIterable));
    }

}
