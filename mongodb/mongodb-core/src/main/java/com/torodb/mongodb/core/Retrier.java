
package com.torodb.mongodb.core;

import com.torodb.common.util.RetryHelper.ExceptionHandler;
import java.util.concurrent.Callable;

/**
 *
 */
public interface Retrier {
    
    public <Result> Result retry(Callable<Result> callable);

    public <Result> Result retry(Callable<Result> callable, Result defaultValue);

    public <Result, T extends Exception> Result retry(Callable<Result> callable, ExceptionHandler<Result, T> handler) throws T;

}
