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

import com.google.common.primitives.Longs;
import com.torodb.kvdocument.types.LongType;

/**
 *
 */
public abstract class KVLong extends KVNumeric<Long> {

    private static final long serialVersionUID = -4342266574537851228L;

    public static KVLong of(long l) {
        if (l == 0) {
            return DefaultKVLong.ZERO;
        }
        if (l == 1) {
            return DefaultKVLong.ONE;
        }
        if (l == -1) {
            return DefaultKVLong.MINUS_ONE;
        }
        return new DefaultKVLong(l);
    }

    @Override
    public LongType getType() {
        return LongType.INSTANCE;
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
        if (!(obj instanceof KVLong)) {
            return false;
        }
        return this.longValue() == ((KVLong) obj).longValue();
    }

    @Override
    public <Result, Arg> Result accept(KVValueVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this, arg);
    }

    private static class DefaultKVLong extends KVLong {

        private static final long serialVersionUID = 1412077251749154561L;

        private static final DefaultKVLong ZERO = new DefaultKVLong(0);
        private static final DefaultKVLong ONE = new DefaultKVLong(1);
        private static final DefaultKVLong MINUS_ONE = new DefaultKVLong(-1);

        private final long value;

        private DefaultKVLong(long value) {
            this.value = value;
        }

        @Override
        public long longValue() {
            return value;
        }

    }
}
