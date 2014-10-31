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

package com.torodb.torod.tools.sequencer;



import com.google.common.base.Throwables;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
@Ignore
public class SequencerTest {

    private final long maxExpectedMillis;
    private static final Logger LOG = Logger.getLogger(SequencerTest.class.getName());
    private final ConcurrentLinkedQueue<Throwable> throwables;

    public SequencerTest(long maxExpectedMillis) {
        this.maxExpectedMillis = maxExpectedMillis;
        this.throwables = new ConcurrentLinkedQueue<Throwable>();
    }

    @Test
    public void test() throws Exception, Throwable {
        test(this, maxExpectedMillis, throwables);
        finish();
    }

    public void finish() throws Exception {
    }

    public static void test(Object testCase, long maxExpectedMillis, ConcurrentLinkedQueue<Throwable> throwables) throws SequencerTimeoutException, Throwable {
        Collection<Thread> threads = new LinkedList<Thread>();
        Class<?> c = testCase.getClass();
        for (Method method : c.getMethods()) {
            if (method.isAnnotationPresent(ConcurrentTest.class)) {
                if (method.getReturnType().equals(Void.class)) {
                    throw new RuntimeException("Thread methods must return void");
                }
                if (!Modifier.isPublic(method.getModifiers())) {
                    throw new RuntimeException("Thread methods must be public");
                }
                if (method.getParameterTypes().length != 0) {
                    throw new RuntimeException("Thread methods must not have parameters");
                }
                threads.add(new Thread(createRunnable(testCase, method, throwables)));
            }
        }
        for (Thread thread : threads) {
            thread.start();
        }
        waitSubtasks(threads, maxExpectedMillis, throwables);

        joinThreads(threads, !throwables.isEmpty());

        if (!throwables.isEmpty()) {
            for (Throwable throwable : throwables) {
                System.err.println(Throwables.getStackTraceAsString(throwable));
            }
            throw throwables.peek();
        }
    }

    @SuppressFBWarnings(value = "UW_UNCOND_WAIT")
    private static void waitSubtasks(Collection<Thread> threads, long maxExpectedMillis, ConcurrentLinkedQueue<Throwable> throwables) {
        long maxTime = System.currentTimeMillis() + maxExpectedMillis;
        final Object lock = new Object();
        while (maxTime > System.currentTimeMillis()) {
            if (!throwables.isEmpty()) {
                break;
            }

            boolean anyAlive = false;
            for (Thread thread : threads) {
                if (thread.isAlive()) {
                    anyAlive = true;
                    break;
                }
            }
            if (anyAlive) {
                synchronized (lock) {
                    try {
                        lock.wait(100l);
                    } catch (InterruptedException ex) {
                        Logger.getLogger(Sequencer.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }
            else {
                break;
            }
        }
    }

    private static void joinThreads(Collection<Thread> threads, boolean previousError) throws SequencerTimeoutException {
        Collection<Thread> blockedThreads = new LinkedList<Thread>();

        for (Thread thread : threads) {
            if (thread.isAlive()) {
                LOG.log(Level.SEVERE, "Interrupting thread ''{0}''.", thread);
                thread.interrupt();
                blockedThreads.add(thread);
                try {
                    LOG.severe("Waiting for " + thread.getName());
                    thread.join();
                } catch (InterruptedException ex) {
                    LOG.log(Level.SEVERE, null, ex);
                }
            }
        }

        if (!previousError && !blockedThreads.isEmpty()) {
            throw new SequencerTimeoutException(blockedThreads);
        }
    }

    private static Runnable createRunnable(final Object testCase, final Method m, final ConcurrentLinkedQueue<Throwable> throwables) {
        return new Runnable() {

            @Override
            public void run() {
                try {
                    Thread.currentThread().setName("Method '" + m.getName() + "'");
                    m.invoke(testCase);
                } catch (IllegalAccessException ex) {
                    throwables.add(ex);
                } catch (IllegalArgumentException ex) {
                    throwables.add(ex);
                } catch (InvocationTargetException ex) {
                    if (ex.getCause() instanceof InterruptedExceptionRuntimeException) {
                        LOG.log(Level.SEVERE, "Thread ''{0}'' has been interrupted", Thread.currentThread());
                    } else {
                        if (ex.getCause() instanceof RuntimeException || ex.getCause() instanceof Error) {
                            throwables.add(ex.getCause());
                        } else {
                            throwables.add(ex);
                        }
                    }
                }
            }
        };
    }

}
