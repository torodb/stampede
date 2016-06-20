
package com.torodb.core;

import com.torodb.common.util.RetryHelper.ExceptionHandler;
import com.torodb.core.exceptions.user.UserException;
import java.util.concurrent.Callable;

/**
 *
 */
public interface Retrier {

    public <Result> Result retry(Callable<Result> callable);

    public <Result> Result retryOrUserEx(Callable<Result> callable) throws UserException;

    public <Result> Result retry(Callable<Result> callable, Result defaultValue);

    public <Result, T extends Exception> Result retry(Callable<Result> callable, ExceptionHandler<Result, T> handler) throws T;

}
