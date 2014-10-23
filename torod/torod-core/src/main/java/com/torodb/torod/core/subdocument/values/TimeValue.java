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

package com.torodb.torod.core.subdocument.values;

import com.torodb.torod.core.subdocument.BasicType;
import org.threeten.bp.LocalTime;

/**
 *
 */
public class TimeValue implements Value<LocalTime> {
    private static final long serialVersionUID = 1L;

    private final LocalTime value;

    public TimeValue(LocalTime value) {
        this.value = value;
    }

    @Override
    public LocalTime getValue() {
        return value;
    }

    @Override
    public BasicType getType() {
        return BasicType.DATETIME;
    }

    @Override
    public String toString() {
        return value.toString();
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 67 * hash + (this.value != null ? this.value.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final TimeValue other = (TimeValue) obj;
        if ((this.value == null) ? (other.value != null) : !this.value.equals(
            other.value)) {
            return false;
        }
        return true;
    }

    @Override
    public <Result, Arg> Result accept(ValueVisitor<Result, Arg> visitor,
                                       Arg arg) {
        return visitor.visit(this, arg);
    }
    
}
