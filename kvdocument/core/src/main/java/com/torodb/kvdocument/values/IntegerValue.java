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
import com.torodb.kvdocument.types.IntegerType;
import com.torodb.kvdocument.types.LongType;

/**
 *
 */
public class IntegerValue implements NumericDocValue {
    private final int value;

    public IntegerValue(int value) {
        this.value = value;
    }

    @Override
    public Integer getValue() {
        return value;
    }

    @Override
    public IntegerType getType() {
        return IntegerType.INSTANCE;
    }

    @Override
    public NumericDocValue increment(NumericDocValue other) {
        if (other.getType().equals(DoubleType.INSTANCE)) {
            return new DoubleValue(value + other.getValue().doubleValue());
        }
        if (other.getType().equals(LongType.INSTANCE)) {
            return new LongValue(value + other.getValue().longValue());
        }
        return new IntegerValue(value + other.getValue().intValue());
    }

    @Override
    public NumericDocValue multiply(NumericDocValue other) {
        if (other.getType().equals(DoubleType.INSTANCE)) {
            return new DoubleValue(value * other.getValue().doubleValue());
        }
        if (other.getType().equals(LongType.INSTANCE)) {
            return new LongValue(value * other.getValue().longValue());
        }
        return new IntegerValue(value * other.getValue().intValue());
    }
    
    @Override
    public String toString() {
        return Integer.toString(value);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + this.value;
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
        final IntegerValue other = (IntegerValue) obj;
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
