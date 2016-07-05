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

package com.torodb.integration.backend;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.torodb.backend.MockDidCursor;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import com.torodb.core.transaction.metainf.WrapperMutableMetaSnapshot;
import com.torodb.kvdocument.values.KVDocument;

public class BackendRollbackIntegrationTest extends AbstractBackendTest {

    private final static Logger LOGGER = LogManager.getLogger(BackendRollbackIntegrationTest.class);
    
    @Rule
    public RollbackOrSuccessRule rule = new RollbackOrSuccessRule();
    
    private StepThread leftThread;
    private StepThread rightThread;
    
    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        
        leftThread = new StepThread("left");
        rightThread = new StepThread("right");
        leftThread.start();
        rightThread.start();
    }
    
    @After
    public void tearDown() throws Throwable {
        leftThread.terminate();
        rightThread.terminate();
    }
    
    private static class RollbackOrSuccessRule implements TestRule {

        public boolean couldRollback = false;
        
        public void couldRollback() {
            couldRollback = true;
        }
        
        @Override
        public Statement apply(Statement base, Description description) {
            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    try {
                        base.evaluate();
                        
                        LOGGER.info("No exception was thrown");
                    } catch(RollbackException rollbackException) {
                        String sqlCode = null;
                        String message = null;
                        if (rollbackException.getCause() instanceof SQLException) {
                            sqlCode = ((SQLException) rollbackException.getCause()).getSQLState();
                            message = ((SQLException) rollbackException.getCause()).getMessage();
                        } else
                        if (rollbackException.getCause() instanceof DataAccessException) {
                            sqlCode = ((DataAccessException) rollbackException.getCause()).sqlState();
                            message = ((DataAccessException) rollbackException.getCause()).getMessage();
                        }
                        if (sqlCode != null) {
                            LOGGER.info("Rollback on SQLState: " + sqlCode + " Message: " + message);
                        } else {
                            LOGGER.info("Custom rollback");
                        }
                        if (!couldRollback) {
                            throw rollbackException;
                        }
                    } catch(SQLException sqlException) {
                        SQLException nextedSqlException = sqlException;
                        do {
                            LOGGER.error("SQLState: " + nextedSqlException.getSQLState() + " Message: " + nextedSqlException.getMessage());
                            nextedSqlException = nextedSqlException.getNextException();
                        } while(nextedSqlException != null);
                        
                        throw sqlException;
                    } catch(DataAccessException dataAccessException) {
                        LOGGER.error("SQLState: " + dataAccessException.sqlState() + " Message: " + dataAccessException.getMessage());
                        
                        throw dataAccessException;
                    }
                }
            };
        }
        
    }
    
    @Test
    public void testCreateSameSchema() throws Throwable {
        try (
             Connection leftConnection = sqlInterface.getDbBackend().createWriteConnection();
             Connection rightConnection = sqlInterface.getDbBackend().createWriteConnection()
            ) {
            DSLContext leftDsl = sqlInterface.getDslContextFactory().createDSLContext(leftConnection);
            DSLContext rightDsl = sqlInterface.getDslContextFactory().createDSLContext(rightConnection);
            leftThread.step(() -> sqlInterface.getStructureInterface().createSchema(leftDsl, "test"))
                .waitQueuedSteps();
            rightThread.step(() -> sqlInterface.getStructureInterface().createSchema(rightDsl, "test"))
                .waitUntilThreadSleeps(1);
            leftThread.step(() -> leftConnection.commit())
                .waitQueuedSteps();
            rule.couldRollback();
            rightThread.step(() -> rightConnection.commit())
                .waitQueuedSteps();
        }
    }
    
    @Test
    public void testCreateSameTable() throws Throwable {
        try (Connection connection = sqlInterface.getDbBackend().createWriteConnection()) {
            DSLContext dsl = sqlInterface.getDslContextFactory().createDSLContext(connection);
            sqlInterface.getStructureInterface().createSchema(dsl, "test");
            connection.commit();
        }
        
        try (
             Connection leftConnection = sqlInterface.getDbBackend().createWriteConnection();
             Connection rightConnection = sqlInterface.getDbBackend().createWriteConnection()
            ) {
            DSLContext leftDsl = sqlInterface.getDslContextFactory().createDSLContext(leftConnection);
            DSLContext rightDsl = sqlInterface.getDslContextFactory().createDSLContext(rightConnection);
            leftThread.step(() -> sqlInterface.getStructureInterface().createRootDocPartTable(leftDsl, "test", "test", createTableRef()))
                .waitQueuedSteps();
            rightThread.step(() -> sqlInterface.getStructureInterface().createRootDocPartTable(rightDsl, "test", "test", createTableRef()))
                .waitUntilThreadSleeps(1);
            leftThread.step(() -> leftConnection.commit()).waitQueuedSteps();
            rule.couldRollback();
            rightThread.step(() -> rightConnection.commit()).waitQueuedSteps();
        }
    }
    
    @Test
    public void testCreateSameColumn() throws Throwable {
        try (Connection connection = sqlInterface.getDbBackend().createWriteConnection()) {
            DSLContext dsl = sqlInterface.getDslContextFactory().createDSLContext(connection);
            sqlInterface.getStructureInterface().createSchema(dsl, "test");
            sqlInterface.getStructureInterface().createRootDocPartTable(dsl, "test", "test", createTableRef());
            connection.commit();
        }
        
        try (
             Connection leftConnection = sqlInterface.getDbBackend().createWriteConnection();
             Connection rightConnection = sqlInterface.getDbBackend().createWriteConnection()
            ) {
            DSLContext leftDsl = sqlInterface.getDslContextFactory().createDSLContext(leftConnection);
            DSLContext rightDsl = sqlInterface.getDslContextFactory().createDSLContext(rightConnection);
            leftThread.step(() -> sqlInterface.getStructureInterface()
                    .addColumnToDocPartTable(leftDsl, "test", "test", "test", sqlInterface.getDataTypeProvider().getDataType(FieldType.STRING)))
                .waitQueuedSteps();
            rightThread.step(() -> sqlInterface.getStructureInterface()
                    .addColumnToDocPartTable(rightDsl, "test", "test", "test", sqlInterface.getDataTypeProvider().getDataType(FieldType.STRING)))
                .waitUntilThreadSleeps(1);
            leftThread.step(() -> leftConnection.commit()).waitQueuedSteps();
            rule.couldRollback();
            rightThread.step(() -> rightConnection.commit()).waitQueuedSteps();
        }
    }
    
    @Test
    public void testDeletedSameData() throws Throwable {
        List<Integer> generatedDids;
        ImmutableMetaDocPart metaDocPart = new ImmutableMetaDocPart.Builder(createTableRef(), "test").build();
        ImmutableMetaCollection metaCollection = new ImmutableMetaCollection.Builder("test", "test")
                .add(metaDocPart)
                .build();
        ImmutableMetaDatabase metaDatabase = new ImmutableMetaDatabase.Builder("test", "test")
                .add(metaCollection)
                .build();
        MutableMetaSnapshot mutableSnapshot = new WrapperMutableMetaSnapshot(new ImmutableMetaSnapshot.Builder()
                .add(metaDatabase)
                .build());
        try (Connection connection = sqlInterface.getDbBackend().createWriteConnection()) {
            DSLContext dsl = sqlInterface.getDslContextFactory().createDSLContext(connection);
            sqlInterface.getStructureInterface().createSchema(dsl, "test");
            sqlInterface.getStructureInterface().createRootDocPartTable(dsl, "test", "test", createTableRef());
            
            generatedDids = writeCollectionData(dsl, "test", parseDocuments(mutableSnapshot, "test", "test", dsl, 
                    ImmutableList.<KVDocument>of(new KVDocument.Builder().build())));
            connection.commit();
        }
        
        try (
             Connection leftConnection = sqlInterface.getDbBackend().createWriteConnection();
             Connection rightConnection = sqlInterface.getDbBackend().createWriteConnection()
            ) {
            DSLContext leftDsl = sqlInterface.getDslContextFactory().createDSLContext(leftConnection);
            DSLContext rightDsl = sqlInterface.getDslContextFactory().createDSLContext(rightConnection);
            
            rightThread.step(() -> sqlInterface.getReadInterface().getAllCollectionDids(rightDsl, metaDatabase, metaCollection))
                .waitQueuedSteps();
            leftThread.step(() -> sqlInterface.getWriteInterface().deleteCollectionDocParts(leftDsl, "test", metaCollection, 
                    new MockDidCursor(generatedDids.iterator())))
                .waitQueuedSteps();
            rightThread.step(() -> sqlInterface.getWriteInterface().deleteCollectionDocParts(rightDsl, "test", metaCollection, 
                    new MockDidCursor(generatedDids.iterator())))
                .waitUntilThreadSleeps(1);
            leftThread.step(() -> leftConnection.commit())
                .waitQueuedSteps();
            rule.couldRollback();
            rightThread.step(() -> rightConnection.commit())
                .waitQueuedSteps();
        }
    }
    
    @FunctionalInterface
    private interface Step {
        public void apply() throws Exception;
    }
    
    private class StepThread extends Thread {
        
        private boolean run = true;
        private List<Step> stepQueue = new ArrayList<>();
        private Throwable throwable;
        
        private StepThread(String name) {
            super(name);
        }

        public void run() {
            while (run) {
                synchronized (this) {
                    while (run && stepQueue.isEmpty()) {
                        try {
                            wait(100);
                        } catch(InterruptedException interruptedException) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }

                synchronized (this) {
                    if (run) {
                        try {
                            stepQueue.remove(0).apply();
                        } catch(Throwable throwable) {
                            this.throwable = throwable;
                            stepQueue.clear();
                        }
                    }
                    
                    notifyAll();
                }
            }
        }
        
        public StepThread step(Step step) {
            Preconditions.checkArgument(run);
            
            synchronized (this) {
                this.stepQueue.add(step);
                
                notifyAll();
            }
            
            return this;
        }
        
        public void waitUntilThreadSleeps(int timeoutSeconds) {
            Instant until = Instant.now().plus(timeoutSeconds, ChronoUnit.SECONDS);
            while (getState() != State.TIMED_WAITING &&
                    getState() != State.WAITING &&
                    Instant.now().isAfter(until)) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            
            if (Instant.now().isAfter(until)) {
                Assert.fail("Timed out after " + timeoutSeconds 
                        + " seconds while waiting for thread " 
                        + getName() + " to go to sleep");
            }
            
            try {
                Thread.sleep(Duration.between(Instant.now(), until).toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        public void waitQueuedSteps() throws Throwable {
            waitQueuedSteps(true);
        }
        
        public void terminate() throws Throwable {
            waitQueuedSteps(false);
            
            synchronized (this) {
                run = false;
                
                notifyAll();
            }
        }
        
        private void waitQueuedSteps(boolean throwThrowable) throws Throwable {
            synchronized (this) {
                while (run && !stepQueue.isEmpty()) {
                    try {
                        wait(100);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
            
            if (throwThrowable && throwable != null) {
                throw throwable;
            }
        }
    }
}
