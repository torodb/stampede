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

package com.torodb.core.exceptions.user;

import com.torodb.kvdocument.values.KVValue;
import javax.annotation.Nullable;

/**
 *
 */
public class UniqueIndexViolationException extends IndexException {

    private static final long serialVersionUID = 1;

    @Nullable
    private final KVValue<?> repeatedValue;

    public UniqueIndexViolationException(String index, KVValue<?> repeatedValue) {
        super(null, null, index);
        this.repeatedValue = repeatedValue;
    }

    public UniqueIndexViolationException(String index, KVValue<?> repeatedValue, String message) {
        super(message, null, null, index);
        this.repeatedValue = repeatedValue;
    }

    public UniqueIndexViolationException(String index, KVValue<?> repeatedValue, String message, Throwable cause) {
        super(message, cause, null, null, index);
        this.repeatedValue = repeatedValue;
    }

    public UniqueIndexViolationException(String message) {
        super(message, null, null, null);
        this.repeatedValue = null;
    }

    public UniqueIndexViolationException(String message, Throwable cause) {
        super(message, cause, null, null, null);
        this.repeatedValue = null;
    }

    @Nullable
    public KVValue<?> getRepeatedValue() {
        return repeatedValue;
    }

    @Override
    public <Result, Argument> Result accept(UserExceptionVisitor<Result, Argument> visitor, Argument arg) {
        return visitor.visit(this, arg);
    }

}
