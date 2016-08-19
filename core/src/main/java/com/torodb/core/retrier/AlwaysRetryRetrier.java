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

package com.torodb.core.retrier;

import com.torodb.common.util.RetryHelper;
import com.torodb.common.util.RetryHelper.ExceptionHandler;
import java.util.EnumSet;

/**
 * A retrier that never gives up
 */
public class AlwaysRetryRetrier extends AbstractHintableRetrier {

    private static final AlwaysRetryRetrier INSTANCE = new AlwaysRetryRetrier();

    private AlwaysRetryRetrier() {}

    @Override
    protected <Result, T extends Exception> ExceptionHandler<Result, T> getExceptionHandler(EnumSet<Hint> hints, ExceptionHandler<Result, T> delegateHandler) {
        return RetryHelper.alwaysRetryHandler();
    }

    public static AlwaysRetryRetrier getInstance() {
        return INSTANCE;
    }

}
