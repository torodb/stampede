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

package com.torodb.torod.core.dbWrapper.exceptions;

import java.sql.SQLTransientException;

/**
 *
 */
public class DbException extends Exception {

    private static final long serialVersionUID = 1L;

    private final boolean _transient;

    public DbException(boolean _transient) {
        this._transient = _transient;
    }

    public DbException(boolean _transient, String message) {
        super(message);
        this._transient = _transient;
    }

    public DbException(boolean _transient, String message, Throwable cause) {
        super(message, cause);
        this._transient = _transient;
    }

    public DbException(boolean _transient, Throwable cause) {
        super(cause);
        this._transient = _transient;
    }

    /**
     *
     * @return true in situations where the failed operation might be able to succeed when the operation is retried
     *         without any intervention by application-level functionality.
     * @see SQLTransientException
     */
    public boolean isTransient() {
        return _transient;
    }

}
