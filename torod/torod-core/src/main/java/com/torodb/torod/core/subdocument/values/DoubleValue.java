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
import javax.annotation.concurrent.Immutable;


/**
 *
 */
@Immutable
public class DoubleValue implements NumericValue<Double> {
    private static final long serialVersionUID = 1L;
    private final double value;

    public DoubleValue(double value) {
        this.value = value;
    }

    @Override
    public Double getValue() {
        return value;
    }

    @Override
    public BasicType getType() {
        return BasicType.DOUBLE;
    }

    @Override
    public String toString() {
        return Double.toString(value)+ "d";
    }

    @Override
    public NumericValue<? extends Number> add(NumericValue<? extends Number> other) {
        return new DoubleValue(value + other.getValue().doubleValue());
    }

    @Override
    public NumericValue<? extends Number> multiply(NumericValue<? extends Number> other) {
        return new DoubleValue(value * other.getValue().doubleValue());
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 97 * hash + (int) (Double.doubleToLongBits(this.value) ^ (Double.doubleToLongBits(this.value) >>> 32));
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
        final DoubleValue other = (DoubleValue) obj;
        if (Double.doubleToLongBits(this.value) != Double.doubleToLongBits(other.value)) {
            return false;
        }
        return true;
    }

    @Override
    public <Result, Arg> Result accept(ValueVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this, arg);
    }
    
}
