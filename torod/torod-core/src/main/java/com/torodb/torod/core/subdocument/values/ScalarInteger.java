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

import com.torodb.torod.core.subdocument.ScalarType;
import javax.annotation.concurrent.Immutable;

/**
 *
 */
@Immutable
public abstract class ScalarInteger extends ScalarNumeric<Integer> {

    private static final long serialVersionUID = -8056479643235327356L;

    public static ScalarInteger of(int i) {
        if (i == 0) {
            return DefaultScalarInteger.ZERO;
        }
        if (i == 1) {
            return DefaultScalarInteger.ONE;
        }
        if (i == -1) {
            return DefaultScalarInteger.MINUS_ONE;
        }
        return new DefaultScalarInteger(i);
    }

    @Override
    public ScalarType getType() {
        return ScalarType.INTEGER;
    }

    @Override
    public Integer getValue() {
        return intValue();
    }

    @Override
    public long longValue() {
        return intValue();
    }

    @Override
    public double doubleValue() {
        return intValue();
    }

    @Override
    public Class<? extends Integer> getValueClass() {
        return Integer.class;
    }

    @Override
    public String toString() {
        return getValue().toString();
    }

    @Override
    public int hashCode() {
        return intValue();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof ScalarInteger)) {
            return false;
        }
        return this.intValue() == ((ScalarInteger) obj).intValue();
    }

    @Override
    public <Result, Arg> Result accept(ScalarValueVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this, arg);
    }

    private static class DefaultScalarInteger extends ScalarInteger {

        private static final long serialVersionUID = -3509381713256279595L;
        private static final DefaultScalarInteger ZERO = new DefaultScalarInteger(0);
        private static final DefaultScalarInteger ONE = new DefaultScalarInteger(1);
        private static final DefaultScalarInteger MINUS_ONE = new DefaultScalarInteger(-1);

        private final int value;

        private DefaultScalarInteger(int value) {
            this.value = value;
        }

        @Override
        public int intValue() {
            return value;
        }
    }
}
