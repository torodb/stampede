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

import com.google.common.primitives.Longs;
import com.torodb.torod.core.subdocument.ScalarType;
import javax.annotation.concurrent.Immutable;

/**
 *
 */
@Immutable
public abstract class ScalarLong extends ScalarNumeric<Long> {

    private static final long serialVersionUID = -4342266574537851228L;

    public static ScalarLong of(long l) {
        if (l == 0) {
            return DefaultScalarLong.ZERO;
        }
        if (l == 1) {
            return DefaultScalarLong.ONE;
        }
        if (l == -1) {
            return DefaultScalarLong.MINUS_ONE;
        }
        return new DefaultScalarLong(l);
    }

    @Override
    public ScalarType getType() {
        return ScalarType.LONG;
    }

    @Override
    public Long getValue() {
        return longValue();
    }

    @Override
    public int intValue() {
        return (int) longValue();
    }

    @Override
    public double doubleValue() {
        return longValue();
    }

    @Override
    public Class<? extends Long> getValueClass() {
        return Long.class;
    }

    @Override
    public String toString() {
        return getValue().toString();
    }

    @Override
    public int hashCode() {
        return Longs.hashCode(longValue());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof ScalarLong)) {
            return false;
        }
        return this.longValue() == ((ScalarLong) obj).longValue();
    }

    @Override
    public <Result, Arg> Result accept(ScalarValueVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this, arg);
    }

    private static class DefaultScalarLong extends ScalarLong {

        private static final long serialVersionUID = 4421073405577366968L;

        private static final DefaultScalarLong ZERO = new DefaultScalarLong(0);
        private static final DefaultScalarLong ONE = new DefaultScalarLong(1);
        private static final DefaultScalarLong MINUS_ONE = new DefaultScalarLong(-1);

        private final long value;

        private DefaultScalarLong(long value) {
            this.value = value;
        }

        @Override
        public long longValue() {
            return value;
        }

    }
}
