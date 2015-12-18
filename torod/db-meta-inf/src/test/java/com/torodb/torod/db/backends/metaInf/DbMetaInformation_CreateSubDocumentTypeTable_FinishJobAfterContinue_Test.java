/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.torodb.torod.db.backends.metaInf;

import com.torodb.torod.core.backend.DbBackend;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.executor.ExecutorFactory;
import com.torodb.torod.core.executor.SessionExecutor;
import com.torodb.torod.core.executor.SystemExecutor;
import com.torodb.torod.core.executor.ToroTaskExecutionException;
import com.torodb.torod.core.subdocument.BasicType;
import com.torodb.torod.core.subdocument.SubDocAttribute;
import com.torodb.torod.core.subdocument.SubDocType;
import com.torodb.torod.tools.sequencer.ConcurrentTest;
import com.torodb.torod.tools.sequencer.Sequencer;
import com.torodb.torod.tools.sequencer.SequencerTest;
import java.util.concurrent.Future;
import javax.json.JsonObject;
import org.junit.Before;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 *
 */
public class DbMetaInformation_CreateSubDocumentTypeTable_FinishJobAfterContinue_Test extends SequencerTest {

    private static final String COLLECTION = "test";
    private static final int IDS_TO_RESERVE = 32;

    private SubDocType type;
    private ExecutorFactory executorFactory;
    private SystemExecutor systemExecutor;
    private ReservedIdHeuristic idHeuristic;
    private ReservedIdInfoFactory tableMetaInfoFactory;
    private DefaultDbMetaInformationCache cache;
    private Sequencer<MySteps> sequencer;
    private Future<?> future;

    public DbMetaInformation_CreateSubDocumentTypeTable_FinishJobAfterContinue_Test() {
        super(10000l);
    }

    @Before
    public void setUp() throws ToroTaskExecutionException, ImplementationDbException {
        sequencer = new Sequencer<MySteps>(MySteps.class);
        future = Mockito.mock(Future.class);
        Mockito.doReturn(false).when(future).isDone();

        type = new SubDocType.Builder().add(new SubDocAttribute("att1", BasicType.STRING)).build();

        createSystemExecutor();
        createExecutorFactory();

        idHeuristic = spy(new ReservedIdHeuristic() {

            @Override
            public int evaluate(int lastUsedId, int lastCachedId) {
                return IDS_TO_RESERVE;
            }
        });
        tableMetaInfoFactory = spy(new DefaultTableMetaInfoFactory());

        createCache();
    }

    private void createCache() {
        DbBackend config = mock(DbBackend.class, new ThrowExceptionAnswer());
        Mockito.doReturn(64).when(config).getCacheSubDocTypeStripes();
        cache = new DefaultDbMetaInformationCache(executorFactory, idHeuristic, tableMetaInfoFactory);

        cache.createCollection(null, COLLECTION, null);
    }

    private SessionExecutor createSessionExecutor() {
        SessionExecutor executor = mock(SessionExecutor.class, new ThrowExceptionAnswer());
        Mockito.doNothing().when(executor).pauseUntil(any(long.class));

        return executor;
    }

    private void createSystemExecutor() throws ToroTaskExecutionException {
        systemExecutor = mock(SystemExecutor.class, new ThrowExceptionAnswer());
        Mockito.doAnswer(new Answer() {

            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                return whenCreateSubDocTable();
            }
        }).when(systemExecutor).createSubDocTable(any(String.class), any(SubDocType.class), any(SystemExecutor.CreateSubDocTypeTableCallback.class));
        Mockito.doAnswer(new Answer() {

            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                return whenCreateCollection(COLLECTION, (SystemExecutor.CreateCollectionCallback) invocation.getArguments()[2]);
            }
        }).when(systemExecutor).createCollection(any(String.class), any(JsonObject.class), any(SystemExecutor.CreateCollectionCallback.class));
        Mockito.doReturn(1l).when(systemExecutor).getTick();
    }

    private void createExecutorFactory() throws ImplementationDbException {
        executorFactory = Mockito.mock(ExecutorFactory.class);
        Mockito.doNothing().when(executorFactory).initialize();
        Mockito.doReturn(systemExecutor).when(executorFactory).getSystemExecutor();
    }

    private Future<?> whenCreateSubDocTable() {
        return future;
    }

    private Future<?> whenCreateCollection(String collection, SystemExecutor.CreateCollectionCallback callback) {
        callback.createdCollection(collection);
        Future<Object> f = Mockito.mock(Future.class);
        Mockito.doReturn(true).when(f).isDone();

        return f;
    }

    @Override
    public void finish() throws ToroTaskExecutionException {
        verify(executorFactory, Mockito.atLeastOnce())
                .getSystemExecutor();

        verify(systemExecutor)
                .createSubDocTable(
                        eq(COLLECTION),
                        eq(type),
                        any(SystemExecutor.CreateSubDocTypeTableCallback.class));

        verify(systemExecutor)
                .createCollection(eq(COLLECTION), any(JsonObject.class), any(SystemExecutor.CreateCollectionCallback.class));
        verify(systemExecutor, Mockito.atLeast(1))
                .getTick();
        verifyNoMoreInteractions(systemExecutor);
    }

    @ConcurrentTest
    public void executorThread() {
        sequencer.waitFor(MySteps.JOB_CONSUMED);
        Mockito.doReturn(true).when(future).isDone();
    }

    @ConcurrentTest
    public void cacheThread() {
        cache.createSubDocTypeTable(createSessionExecutor(), COLLECTION, type);
        sequencer.notify(MySteps.JOB_CONSUMED);
    }

    private enum MySteps {

        JOB_CREATED,
        JOB_CONSUMED
    }

}
