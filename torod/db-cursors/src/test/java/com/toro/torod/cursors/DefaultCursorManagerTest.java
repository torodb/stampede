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

package com.toro.torod.cursors;

import com.google.common.base.Preconditions;
import com.google.common.base.Ticker;
import com.torodb.torod.core.Session;
import com.torodb.torod.core.config.TorodConfig;
import com.torodb.torod.core.cursors.CursorId;
import com.torodb.torod.core.cursors.CursorProperties;
import com.torodb.torod.core.executor.SessionExecutor;
import com.torodb.torod.core.executor.SessionTransaction;
import com.torodb.torod.core.executor.ToroTaskExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.internal.stubbing.answers.ThrowsException;

import static org.mockito.Mockito.*;

/**
 *
 */
public class DefaultCursorManagerTest {

    private final TorodConfig config;
    private final Session session1 = mock(Session.class);
    private final Session session2 = new MyConnectionId();
    private final long timeout = 1000 * 60 * 60; //in milliseconds

    public DefaultCursorManagerTest() {
        config = mock(TorodConfig.class, new ThrowsException(new AssertionError()));
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testCreateCursor_basic() {
        doReturn(timeout).when(config).getDefaultCursorTimeout();
        SessionTransaction sessionTransaction = mock(SessionTransaction.class, new ThrowsException(new AssertionError()));

        DefaultCursorManager cursorManager = new DefaultCursorManager(config, sessionTransaction);

        CursorProperties createdCursor1 = cursorManager.createUnlimitedCursor(session1, false, false);
        assert createdCursor1.getOwner().equals(session1);
        assert createdCursor1.getId() != null;
        assert createdCursor1.hasLimit() == false;
        assert createdCursor1.hasTimeout() == false;

        CursorProperties createdCursor2 = cursorManager.createLimitedCursor(session2, true, true, 1000);
        assert createdCursor2.getOwner().equals(session2);
        assert createdCursor2.getId() != null;
        assert createdCursor2.getLimit() == 1000;
        assert createdCursor2.hasLimit() == true;
        assert createdCursor2.hasTimeout() == true;
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateLimitedCursor_negativeLimit() {
        doReturn(Long.MAX_VALUE).when(config).getDefaultCursorTimeout();
        SessionTransaction sessionTransaction = mock(SessionTransaction.class, new ThrowsException(new AssertionError()));

        DefaultCursorManager cursorManager = new DefaultCursorManager(config, sessionTransaction);

        cursorManager.createLimitedCursor(session1, false, false, -100);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateLimitedCursor_zeroLimit() {
        doReturn(Long.MAX_VALUE).when(config).getDefaultCursorTimeout();
        SessionTransaction sessionTransaction = mock(SessionTransaction.class, new ThrowsException(new AssertionError()));

        DefaultCursorManager cursorManager = new DefaultCursorManager(config, sessionTransaction);

        cursorManager.createLimitedCursor(session1, false, false, 0);
    }

    @Test
    public void testCreateCursor_different() {
        doReturn(Long.MAX_VALUE).when(config).getDefaultCursorTimeout();
        SessionTransaction sessionTransaction = mock(SessionTransaction.class, new ThrowsException(new AssertionError()));

        DefaultCursorManager cursorManager = new DefaultCursorManager(config, sessionTransaction);

        CursorProperties createdCursor1 = cursorManager.createUnlimitedCursor(session1, false, false);
        CursorProperties createdCursor2 = cursorManager.createUnlimitedCursor(session1, false, false);

        assert createdCursor1.getId().equals(createdCursor2.getId()) == false : "Two different cursos has the same id!";
    }

    @Test
    public void testExists() {
        doReturn(Long.MAX_VALUE).when(config).getDefaultCursorTimeout();
        SessionTransaction sessionTransaction = mock(SessionTransaction.class, new ThrowsException(new AssertionError()));

        DefaultCursorManager cursorManager = new DefaultCursorManager(config, sessionTransaction);

        CursorProperties createdCursor1 = cursorManager.createUnlimitedCursor(session1, false, false);

        assert cursorManager.exists(createdCursor1.getId());
    }

    @Test
    public void testClose() throws ToroTaskExecutionException {
        doReturn(Long.MAX_VALUE).when(config).getDefaultCursorTimeout();
        SessionTransaction sessionTransaction = mock(SessionTransaction.class);

        DefaultCursorManager cursorManager = new DefaultCursorManager(config, sessionTransaction);

        CursorProperties createdCursor1 = cursorManager.createUnlimitedCursor(session1, false, false);

        assert cursorManager.exists(createdCursor1.getId()) : "A recently created cursor should be contained in the cursor manager";

        //once a cursor is closed in cursorManager, the executor must be notified
        doReturn(null).when(sessionTransaction).closeCursor(createdCursor1.getId());

        cursorManager.close(createdCursor1.getId());
        assert cursorManager.exists(createdCursor1.getId()) == false : "a closed cursor shouldn't be contained in the cursor manager";

        //executor should only recive one call to closeCursor with the give cursorId
        verify(sessionTransaction, only()).closeCursor(createdCursor1.getId());
    }

    @Test()
    public void testNotifyRead_noLimit() {
        doReturn(Long.MAX_VALUE).when(config).getDefaultCursorTimeout();
        SessionTransaction sessionTransaction = mock(SessionTransaction.class, new ThrowsException(new AssertionError()));

        DefaultCursorManager cursorManager = new DefaultCursorManager(config, sessionTransaction);

        CursorProperties createdCursor1 = cursorManager.createUnlimitedCursor(session1, false, false);

        assert cursorManager.notifyRead(createdCursor1.getId(), 123) == false;
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNotifyRead_negativeReadElements() {
        doReturn(Long.MAX_VALUE).when(config).getDefaultCursorTimeout();
        SessionTransaction sessionTransaction = mock(SessionTransaction.class, new ThrowsException(new AssertionError()));

        DefaultCursorManager cursorManager = new DefaultCursorManager(config, sessionTransaction);

        CursorProperties createdCursor1 = cursorManager.createLimitedCursor(session1, false, true, 1000000);

        cursorManager.notifyRead(createdCursor1.getId(), -123);
    }

    @Test()
    public void test_readLogic() {
        doReturn(Long.MAX_VALUE).when(config).getDefaultCursorTimeout();
        SessionTransaction sessionTransaction = mock(SessionTransaction.class, new ThrowsException(new AssertionError()));

        DefaultCursorManager cursorManager = new DefaultCursorManager(config, sessionTransaction);

        CursorProperties createdCursor1 = cursorManager.createLimitedCursor(session1, false, true, 1000000);

        assert cursorManager.getReadElements(createdCursor1.getId()) == 0;

        cursorManager.notifyRead(createdCursor1.getId(), 123);
        assert cursorManager.getReadElements(createdCursor1.getId()) == 123;

        cursorManager.notifyRead(createdCursor1.getId(), 123444);
        assert cursorManager.getReadElements(createdCursor1.getId()) == 123 + 123444;
    }

    public void testGetReadElements_withoutLimit() {
        doReturn(Long.MAX_VALUE).when(config).getDefaultCursorTimeout();
        SessionTransaction sessionTransaction = mock(SessionTransaction.class, new ThrowsException(new AssertionError()));

        DefaultCursorManager cursorManager = new DefaultCursorManager(config, sessionTransaction);

        CursorProperties createdCursor1 = cursorManager.createUnlimitedCursor(session1, false, true);

        assert cursorManager.getReadElements(createdCursor1.getId()) == 0;
    }

    @Test
    public void testNotifyAllRead_notAutoclose() {
        doReturn(Long.MAX_VALUE).when(config).getDefaultCursorTimeout();
        SessionTransaction sessionTransaction = mock(SessionTransaction.class, new ThrowsException(new AssertionError()));

        DefaultCursorManager cursorManager = new DefaultCursorManager(config, sessionTransaction);

        CursorProperties createdCursor1 = cursorManager.createUnlimitedCursor(session1, false, false);

        assert cursorManager.notifyAllRead(createdCursor1.getId()) == false;
    }

    @Test
    public void testNotifyAllRead_autoclose() throws ToroTaskExecutionException {
        doReturn(Long.MAX_VALUE).when(config).getDefaultCursorTimeout();
        SessionTransaction sessionTransaction = mock(SessionTransaction.class);

        DefaultCursorManager cursorManager = new DefaultCursorManager(config, sessionTransaction);

        CursorProperties createdCursor1 = cursorManager.createUnlimitedCursor(session1, false, true);

        //once a cursor is closed in cursorManager, the executor must be notified
        doReturn(null).when(sessionTransaction).closeCursor(createdCursor1.getId());

        cursorManager.notifyAllRead(createdCursor1.getId());

        assert cursorManager.exists(createdCursor1.getId()) == false : "the cursor should be closed";

        //executor should only recive one call to closeCursor with the give cursorId
        verify(sessionTransaction, only()).closeCursor(createdCursor1.getId());
    }

    @Test
    public void testGetCursor() {
        doReturn(Long.MAX_VALUE).when(config).getDefaultCursorTimeout();
        SessionTransaction sessionTransaction = mock(SessionTransaction.class, new ThrowsException(new AssertionError()));

        DefaultCursorManager cursorManager = new DefaultCursorManager(config, sessionTransaction);

        CursorProperties createdCursor1 = cursorManager.createUnlimitedCursor(session1, false, true);
        assert cursorManager.getCursor(createdCursor1.getId()).equals(createdCursor1);
    }

    @Test
    public void testTimeout() throws ToroTaskExecutionException {
        doReturn(timeout).when(config).getDefaultCursorTimeout();
        
        SessionTransaction sessionTransaction = mock(SessionTransaction.class, new ThrowsException(new AssertionError()));

        FakeTicker ticker = new FakeTicker();

        DefaultCursorManager cursorManager = new DefaultCursorManager(config, sessionTransaction, ticker);

        CursorProperties withoutTimeoutCursor = cursorManager.createUnlimitedCursor(session1, false, false);

        ticker.advance(2 * timeout, TimeUnit.MILLISECONDS);
        CursorProperties notUsedCursor = cursorManager.createUnlimitedCursor(session1, true, false);
        CursorProperties usedCursor = cursorManager.createUnlimitedCursor(session1, true, false);

        doReturn(null).when(sessionTransaction).closeCursor(notUsedCursor.getId());
        doReturn(null).when(sessionTransaction).closeCursor(usedCursor.getId());

        ticker.advance(2 * timeout / 3, TimeUnit.MILLISECONDS);
        assert cursorManager.exists(usedCursor.getId()) == true;

        ticker.advance(2 * timeout / 3, TimeUnit.MILLISECONDS);

        //ticker has advanced 4 / 3 * timeout milliseconds since used and not used limited cursors are created, 2 / 3 since used cursor was used
        CursorProperties recentlyCreatedCursor = cursorManager.createUnlimitedCursor(session1, true, false);
        doReturn(null).when(sessionTransaction).closeCursor(recentlyCreatedCursor.getId());

        assert cursorManager.exists(withoutTimeoutCursor.getId()) == true;
        assert cursorManager.exists(notUsedCursor.getId()) == false;
        assert cursorManager.exists(usedCursor.getId()) == true;
        assert cursorManager.exists(recentlyCreatedCursor.getId()) == true;

        ticker.advance(2 * timeout, TimeUnit.MILLISECONDS);
        assert cursorManager.exists(withoutTimeoutCursor.getId()) == true;
        assert cursorManager.exists(usedCursor.getId()) == false;
        assert cursorManager.exists(recentlyCreatedCursor.getId()) == false;

        /*
         * even if the cursor doesn't exist, the close listener may be not notified until some actions happen.
         */
        CursorProperties token = cursorManager.createUnlimitedCursor(session1, true, true);
        for (int i = 0; i < 100; i++) {
            cursorManager.exists(token.getId());
        }

        verify(sessionTransaction).closeCursor(notUsedCursor.getId());
        verify(sessionTransaction).closeCursor(usedCursor.getId());
        verify(sessionTransaction).closeCursor(recentlyCreatedCursor.getId());
        verifyNoMoreInteractions(sessionTransaction);
    }

    @Test
    public void testDifferentTimeouts() throws ToroTaskExecutionException {
        doReturn(timeout).when(config).getDefaultCursorTimeout();

        SessionTransaction sessionTransaction = mock(SessionTransaction.class, new ThrowsException(new AssertionError()));

        FakeTicker ticker = new FakeTicker();

        DefaultCursorManager cursorManager = new DefaultCursorManager(config, sessionTransaction, ticker);

        CursorProperties t1Cursor = cursorManager.createUnlimitedCursor(session1, true, false);
        doReturn(null).when(sessionTransaction).closeCursor(t1Cursor.getId());

        cursorManager.setTimeout(1000 * timeout);
        CursorProperties t2Cursor = cursorManager.createUnlimitedCursor(session1, true, false);

        ticker.advance(2 * timeout, TimeUnit.MILLISECONDS);
        assert cursorManager.exists(t1Cursor.getId()) == false;
        assert cursorManager.exists(t2Cursor.getId()) == true;
    }

    @Test
    public void testOldCacheEviction() throws ToroTaskExecutionException {
        doReturn(timeout).when(config).getDefaultCursorTimeout();

        SessionTransaction sessionTransaction = mock(SessionTransaction.class, new ThrowsException(new AssertionError()));

        doReturn(null).when(sessionTransaction).closeCursor(any(CursorId.class));

        DefaultCursorManager cursorManager = new DefaultCursorManager(config, sessionTransaction);

        CursorProperties cp1 = cursorManager.createUnlimitedCursor(session1, true, true);

        cursorManager.setTimeout(timeout + 1);

        assert cursorManager.exists(cp1.getId());

        cursorManager.close(cp1.getId());

        for (int i = 0; i < DefaultCursorManager.OLD_CACHE_EVICTION_PERIOD; i++) {
            cursorManager.exists(cp1.getId());
        }

        assert cursorManager.getOldCachesSize() == 0;
    }

    private static class MyConnectionId implements Session {

        private static final long serialVersionUID = 1L;
    }

    public static class FakeTicker extends Ticker {

        private final AtomicLong nanos = new AtomicLong();
        private volatile long autoIncrementStepNanos;

        /**
         * Advances the ticker value by {@code time} in {@code timeUnit}.
         * <p>
         * @param time
         * @param timeUnit
         * @return
         */
        public FakeTicker advance(long time, TimeUnit timeUnit) {
            return advance(timeUnit.toNanos(time));
        }

        /**
         * Advances the ticker value by {@code nanoseconds}.
         * <p>
         * @param nanoseconds
         * @return
         */
        public FakeTicker advance(long nanoseconds) {
            nanos.addAndGet(nanoseconds);
            return this;
        }

        /**
         * Sets the increment applied to the ticker whenever it is queried.
         * <p>
         * <p>
         * The default behavior is to auto increment by zero. i.e: The ticker is left unchanged when queried.
         * <p>
         * @param autoIncrementStep
         * @param timeUnit
         * @return
         */
        public FakeTicker setAutoIncrementStep(long autoIncrementStep, TimeUnit timeUnit) {
            Preconditions.checkArgument(autoIncrementStep >= 0, "May not auto-increment by a negative amount");
            this.autoIncrementStepNanos = timeUnit.toNanos(autoIncrementStep);
            return this;
        }

        @Override
        public long read() {
            return nanos.getAndAdd(autoIncrementStepNanos);
        }
    }

}
