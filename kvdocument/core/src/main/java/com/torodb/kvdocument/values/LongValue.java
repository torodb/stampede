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


package com.torodb.kvdocument.values;

import com.torodb.kvdocument.types.DoubleType;
import com.torodb.kvdocument.types.LongType;

/**
 *
 */
public class LongValue implements NumericDocValue {

    private final long value;

    public LongValue(long value) {
        this.value = value;
    }

    @Override
    public Long getValue() {
        return value;
    }

    @Override
    public LongType getType() {
        return LongType.INSTANCE;
    }

    @Override
    public NumericDocValue increment(NumericDocValue other) {
        if (other.getType().equals(DoubleType.INSTANCE)) {
            return new DoubleValue(value + other.getValue().doubleValue());
        }
        return new LongValue(value + other.getValue().longValue());
    }

    @Override
    public NumericDocValue multiply(NumericDocValue other) {
        if (other.getType().equals(DoubleType.INSTANCE)) {
            return new DoubleValue(value * other.getValue().doubleValue());
        }
        return new LongValue(value * other.getValue().longValue());
    }
    
    @Override
    public String toString() {
        return Long.toString(value);
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 97 * hash + (int) (this.value ^ (this.value >>> 32));
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
        final LongValue other = (LongValue) obj;
        if (this.value != other.value) {
            return false;
        }
        return true;
    }

    @Override
    public <Result, Arg> Result accept(DocValueVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this, arg);
    }
}
