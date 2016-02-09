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

import com.google.common.primitives.Doubles;
import com.torodb.torod.core.subdocument.ScalarType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import javax.annotation.concurrent.Immutable;


/**
 *
 */
@Immutable
public abstract class ScalarDouble extends ScalarNumeric<Double> {

    private static final long serialVersionUID = -3562132855621219615L;

    public static ScalarDouble of(double d) {
        if (d == 0) {
            return DefaultScalarDouble.ZERO;
        }
        if (d == 1) {
            return DefaultScalarDouble.ONE;
        }
        if (d == -1) {
            return DefaultScalarDouble.MINUS_ONE;
        }
        return new DefaultScalarDouble(d);
    }

    @Override
    public ScalarType getType() {
        return ScalarType.DOUBLE;
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
        if (!(obj instanceof ScalarDouble)) {
            return false;
        }
        return this.doubleValue() == ((ScalarDouble) obj).doubleValue();
    }

    @Override
    public <Result, Arg> Result accept(ScalarValueVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this, arg);
    }

    private static class DefaultScalarDouble extends ScalarDouble {

        private static final long serialVersionUID = -95171802921777066L;
        private static final DefaultScalarDouble ZERO = new DefaultScalarDouble(0);
        private static final DefaultScalarDouble ONE = new DefaultScalarDouble(1);
        private static final DefaultScalarDouble MINUS_ONE = new DefaultScalarDouble(-1);
        private final double value;

        public DefaultScalarDouble(double value) {
            this.value = value;
        }

        @Override
        public double doubleValue() {
            return value;
        }
    }
}
