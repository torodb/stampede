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

import com.torodb.kvdocument.types.IntegerType;

/**
 *
 */
public abstract class KVInteger extends KVNumeric<Integer> {

    private static final long serialVersionUID = -8056479643235327356L;

    public static KVInteger of(int i) {
        if (i == 0) {
            return DefaultKVInteger.ZERO;
        }
        if (i == 1) {
            return DefaultKVInteger.ONE;
        }
        if (i == -1) {
            return DefaultKVInteger.MINUS_ONE;
        }
        return new DefaultKVInteger(i);
    }
    
    @Override
    public IntegerType getType() {
        return IntegerType.INSTANCE;
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
        if (!(obj instanceof KVInteger)) {
            return false;
        }
        return this.intValue() == ((KVInteger) obj).intValue();
    }

    @Override
    public <Result, Arg> Result accept(KVValueVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this, arg);
    }

    private static class DefaultKVInteger extends KVInteger {

        private static final long serialVersionUID = 6292206125650070164L;
        private static final DefaultKVInteger ZERO = new DefaultKVInteger(0);
        private static final DefaultKVInteger ONE = new DefaultKVInteger(1);
        private static final DefaultKVInteger MINUS_ONE = new DefaultKVInteger(-1);

        private final int value;

        private DefaultKVInteger(int value) {
            this.value = value;
        }

        @Override
        public int intValue() {
            return value;
        }
    }
}
