/*
 *     This file is part of mongowp.
 *
 *     mongowp is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     mongowp is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with mongowp. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */
package com.torodb.torod.mongodb;

import com.eightkdata.mongowp.exceptions.UnknownErrorException;

/**
 *
 */
public class RetryException extends UnknownErrorException {
    private static final long serialVersionUID = 1L;

    public RetryException(String customMessage) {
        super(customMessage);
    }

    public RetryException() {
        super();
    }

    public RetryException(Throwable cause) {
        super(cause);
    }

    public RetryException(Throwable cause, String customMessage) {
        super(cause, customMessage);
    }
    
}
