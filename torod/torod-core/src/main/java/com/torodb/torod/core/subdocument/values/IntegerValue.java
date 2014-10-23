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
import java.math.BigInteger;
import javax.annotation.concurrent.Immutable;

/**
 *
 */
@Immutable
public class IntegerValue implements NumericValue<Integer> {
    private static final long serialVersionUID = 1L;
    private final int value;

    public IntegerValue(int value) {
        this.value = value;
    }

    @Override
    public Integer getValue() {
        return value;
    }

    @Override
    public BasicType getType() {
        return BasicType.INTEGER;
    }

    @Override
    public NumericValue<? extends Number> add(NumericValue<? extends Number> other) {
        Number otherNumber = other.getValue();
        if (otherNumber instanceof Integer) {
            return new IntegerValue(value + otherNumber.intValue());
        }
        if (otherNumber instanceof Double) {
            return new DoubleValue(value + otherNumber.doubleValue());
        }
        if (otherNumber instanceof Long) {
            return new LongValue(value + otherNumber.longValue());
        }
        throw new AssertionError("The type " + otherNumber.getClass() + " was not expected");
    }

    @Override
    public NumericValue<? extends Number> multiply(NumericValue<? extends Number> other) {
        Number otherNumber = other.getValue();
        if (otherNumber instanceof Integer) {
            return new IntegerValue(value * otherNumber.intValue());
        }
        if (otherNumber instanceof Double) {
            return new DoubleValue(value * otherNumber.doubleValue());
        }
        if (otherNumber instanceof Long) {
            return new LongValue(value * otherNumber.longValue());
        }
        throw new AssertionError("The type " + otherNumber.getClass() + " was not expected");
    }

    @Override
    public String toString() {
        return Integer.toString(value) + "i";
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 89 * hash + this.value;
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
    public <Result, Arg> Result accept(ValueVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this, arg);
    }
}
