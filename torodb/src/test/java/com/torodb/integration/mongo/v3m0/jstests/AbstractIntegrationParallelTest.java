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

package com.torodb.integration.mongo.v3m0.jstests;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.util.List;

import org.slf4j.Logger;

import com.google.common.base.Charsets;

public abstract class AbstractIntegrationParallelTest extends AbstractIntegrationTest {

    public AbstractIntegrationParallelTest(Logger logger) {
        super(logger);
    }
    
    @Override
    protected void runMongoTest(String toroConnectionString, URL mongoMocksUrl, boolean expectedZeroResult,
            boolean exceptionsExpected, String...parameters)
            throws IOException, InterruptedException, UnsupportedEncodingException, AssertionError {
        super.runMongoTest(toroConnectionString, mongoMocksUrl, true, false, "--eval", "var mode='init'");
        
        final int threads = script.getThreads();
        
        ParallalMongoRunner[] parallalMongoRunners = new ParallalMongoRunner[threads];
        
        for (int thread = 0; thread < threads; thread++) {
            parallalMongoRunners[thread] = new ParallalMongoRunner(thread,
                    toroConnectionString, mongoMocksUrl, expectedZeroResult, exceptionsExpected);
        }
        
        for (int thread = 0; thread < threads; thread++) {
            parallalMongoRunners[thread].start();
        }
        
        for (int thread = 0; thread < threads; thread++) {
            parallalMongoRunners[thread].join();
        }
        
        int result = 0;
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        List<Throwable> uncaughtExceptions = TORO_RUNNER_CLASS_RULE.getUcaughtExceptions();
        
        for (int thread = 0; thread < threads; thread++) {
            result = result | parallalMongoRunners[thread].getResult();
            byteArrayOutputStream.write(parallalMongoRunners[thread].getByteArrayOutputStream().toByteArray());
            if (parallalMongoRunners[thread].getThrowable() != null) {
                uncaughtExceptions.add(parallalMongoRunners[thread].getThrowable());
            }
        }
        
        if (!uncaughtExceptions.isEmpty()) {
            PrintStream printStream = new PrintStream(byteArrayOutputStream, true, Charsets.UTF_8.name());
            
            for (Throwable throwable : uncaughtExceptions) {
                throwable.printStackTrace(printStream);
            }
        }

        if (expectedZeroResult) {
            if (result != 0) {
                String reason = new String(byteArrayOutputStream.toByteArray(), Charsets.UTF_8);
                throw new AssertionError("Parallel test " + script + " failed:\n" + reason);
            }
        }
        else {
            if (result == 0) {
                throw new AssertionError("Parallel test " + script + " should fail, but it didn't");
            }
        }

        if (!exceptionsExpected && !uncaughtExceptions.isEmpty()) {
            String reason = new String(byteArrayOutputStream.toByteArray(), Charsets.UTF_8);
            throw new AssertionError("Parallel test " + script + " did not failed but "
                    + "following exception where received:\n" + reason);
        }
    }
    
    private class ParallalMongoRunner extends Thread {
        private final int thread;
        private final String toroConnectionString;
        private final URL mongoMocksUrl;
        private final boolean expectedZeroResult;
        private final boolean exceptionsExpected;
        
        private int result;
        private ByteArrayOutputStream byteArrayOutputStream;
        private Throwable throwable;
        
        public ParallalMongoRunner(int thread, String toroConnectionString, URL mongoMocksUrl, boolean expectedZeroResult,
                boolean exceptionsExpected) {
            super();
            this.thread = thread;
            this.toroConnectionString = toroConnectionString;
            this.mongoMocksUrl = mongoMocksUrl;
            this.expectedZeroResult = expectedZeroResult;
            this.exceptionsExpected = exceptionsExpected;
        }

        public void run() {
            try {
                Process mongoProcess = runMongoProcess(toroConnectionString, mongoMocksUrl, "--eval", "var mode='run', thread=" + thread);
                
                result = mongoProcess.waitFor();
                
                byteArrayOutputStream = readOutput(result, mongoProcess);
            } catch(Throwable throwable) {
                this.throwable = throwable;
            }
        }
        
        public int getResult() {
            return result;
        }

        public ByteArrayOutputStream getByteArrayOutputStream() {
            return byteArrayOutputStream;
        }

        public Throwable getThrowable() {
            return throwable;
        }
    }
}