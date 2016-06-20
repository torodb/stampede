/*
 * This file is part of ToroDB.
 *
 * ToroDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ToroDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with core. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.core.impl;

import com.torodb.common.util.RetryHelper;
import com.torodb.common.util.RetryHelper.ExceptionHandler;
import com.torodb.core.Retrier;
import com.torodb.core.exceptions.user.UserException;
import java.util.concurrent.Callable;

/**
 *
 */
public class RetrierImpl implements Retrier {

    @Override
    public <Result> Result retry(Callable<Result> callable) {
        return RetryHelper.retry(RetryHelper.alwaysRetryHandler(), callable);
    }

    @Override
    public <Result> Result retryOrUserEx(Callable<Result> callable) throws UserException {
        return RetryHelper.retryOrThrow(RetryHelper.<Result, UserException>throwHandler(), callable);
    }

    @Override
    public <Result> Result retry(Callable<Result> callable, Result defaultValue) {
        return RetryHelper.retry(RetryHelper.defaultValueHandler(defaultValue), callable);
    }

    @Override
    public <Result, T extends Exception> Result retry(Callable<Result> callable, ExceptionHandler<Result, T> handler)
            throws T {
        return RetryHelper.retryOrThrow(handler, callable);
    }

}
