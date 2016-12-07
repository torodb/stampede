package com.torodb.common.util;

import org.junit.Test;

import java.util.concurrent.CompletionException;

import static org.junit.Assert.*;

public class CompletionExceptionsTest {

    @Test
    public void firstNonCompletionExceptionIsReturned() throws Exception {
        Throwable nullPointerException = new NullPointerException();
        Throwable completionException = new CompletionException("message", nullPointerException);

        Throwable result = CompletionExceptions.getFirstNonCompletionException(completionException);

        assertEquals(nullPointerException, result);
    }

    @Test
    public void ifHasNoNestedExceptionCompletionExceptionIsReturned() throws Exception {
        Throwable completionException = new CompletionException("message", null);

        Throwable result = CompletionExceptions.getFirstNonCompletionException(completionException);

        assertEquals(completionException, result);
    }

}