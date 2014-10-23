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


package com.torodb.torod.core.exceptions;

/**
 * An exception thrown by an irrecuperable error that doesn't allow to continue the execution of Torod.
 *
 */
public class FatalToroException extends ToroRuntimeException {
    private static final long serialVersionUID = 1L;

    public FatalToroException() {
    }

    public FatalToroException(String message) {
        super(message);
    }

    public FatalToroException(String message, Throwable cause) {
        super(message, cause);
    }

    public FatalToroException(Throwable cause) {
        super(cause);
    }
    
}
