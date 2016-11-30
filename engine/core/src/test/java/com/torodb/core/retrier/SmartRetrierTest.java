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

package com.torodb.core.retrier;

import static org.junit.Assert.assertEquals;

import com.torodb.core.transaction.RollbackException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;

/**
 *
 * @author gortiz
 */
public class SmartRetrierTest {

  private SmartRetrier retrier;
  private final int MAX_EXECUTIONS = 10;

  @Before
  public void setUp() {
    IntPredicate predicate = i -> i >= MAX_EXECUTIONS;
    retrier = new SmartRetrier(predicate, predicate, predicate, predicate, (
        i, millis) -> 2 * millis);
  }

  @Test
  public void testCorrectExecution() throws RetrierGiveUpException {
    AtomicInteger counter = new AtomicInteger(0);

    retrier.retry(() -> {
      return finishSuccessfully(counter);
    });

    assertEquals(1, counter.get());
  }

  @Test
  public void testUncheckedExecution() throws RetrierGiveUpException {
    AtomicInteger counter = new AtomicInteger(0);

    try {
      retrier.retry(() -> {
        return throwUncheckedException(counter);
      });
      Assert.fail("A retrier give up exception was expected");
    } catch (MyRuntimeException ignore) {
    }

    assertEquals(1, counter.get());
  }

  @Test
  public void testError() throws RetrierGiveUpException {
    AtomicInteger counter = new AtomicInteger(0);

    try {
      retrier.retry(() -> {
        return throwError(counter);
      });
      Assert.fail("An error exception was expected");
    } catch (MyError ignore) {
    }

    assertEquals(1, counter.get());
  }

  @Test
  public void testCheckedExecution() throws RetrierGiveUpException {
    AtomicInteger counter = new AtomicInteger(0);

    try {
      retrier.retry(() -> {
        return throwCheckedException(counter);
      });
      Assert.fail("A retrier give up exception was expected");
    } catch (RetrierGiveUpException ignore) {
    }

    assertEquals(MAX_EXECUTIONS, counter.get());
  }

  @Test
  public void testRollbackExecution() throws RetrierGiveUpException {
    AtomicInteger counter = new AtomicInteger(0);

    try {
      retrier.retry(() -> {
        return throwRollbackException(counter);
      });
      Assert.fail("A retrier give up exception was expected");
    } catch (RetrierGiveUpException ignore) {
    }

    assertEquals(MAX_EXECUTIONS, counter.get());
  }

  public static Integer throwCheckedException(AtomicInteger counter) throws Exception {
    counter.incrementAndGet();
    throw new Exception();
  }

  public static Integer throwUncheckedException(AtomicInteger counter) {
    counter.incrementAndGet();
    throw new MyRuntimeException();
  }

  public static Integer throwRollbackException(AtomicInteger counter) {
    counter.incrementAndGet();
    throw new RollbackException();
  }

  public static Integer throwError(AtomicInteger counter) {
    counter.incrementAndGet();
    throw new MyError();
  }

  public static Integer finishSuccessfully(AtomicInteger counter) {
    return counter.incrementAndGet();
  }

  public static class MyRuntimeException extends RuntimeException {

    private static final long serialVersionUID = 58348581394717347L;

  }

  public static class MyError extends Error {
    private static final long serialVersionUID = 5834858139441217347L;
  }

}
