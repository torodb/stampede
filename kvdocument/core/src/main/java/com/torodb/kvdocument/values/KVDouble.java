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

import com.google.common.primitives.Doubles;
import com.torodb.kvdocument.types.DoubleType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 *
 */
public abstract class KVDouble extends KVNumeric<Double> {

    private static final long serialVersionUID = 6351251976353558479L;

    public static KVDouble of(double d) {
        if (d == 0) {
            return DefaultKVDouble.ZERO;
        }
        if (d == 1) {
            return DefaultKVDouble.ONE;
        }
        if (d == -1) {
            return DefaultKVDouble.MINUS_ONE;
        }
        return new DefaultKVDouble(d);
    }

    @Override
    public DoubleType getType() {
        return DoubleType.INSTANCE;
    }

    @Override
    public Double getValue() {
        return doubleValue();
    }

    @Override
    public int intValue() {
        return (int) doubleValue();
    }

    @Override
    public long longValue() {
        return (long) doubleValue();
    }

    @Override
    public Class<? extends Double> getValueClass() {
        return Double.class;
    }

    @Override
    public String toString() {
        return getValue().toString();
    }
    
    @Override
    public int hashCode() {
        return Doubles.hashCode(doubleValue());
    }

    @SuppressFBWarnings(value = "FE_FLOATING_POINT_EQUALITY",
            justification = "We want to check for exactly equality")
    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof KVDouble)) {
            return false;
        }
        return this.doubleValue() == ((KVDouble) obj).doubleValue();
    }

    @Override
    public <Result, Arg> Result accept(KVValueVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this, arg);
    }

    private static class DefaultKVDouble extends KVDouble {

        private static final long serialVersionUID = -4945208796227558609L;
        private static final DefaultKVDouble ZERO = new DefaultKVDouble(0);
        private static final DefaultKVDouble ONE = new DefaultKVDouble(1);
        private static final DefaultKVDouble MINUS_ONE = new DefaultKVDouble(-1);
        private final double value;

        public DefaultKVDouble(double value) {
            this.value = value;
        }

        @Override
        public double doubleValue() {
            return value;
        }
    }
}
