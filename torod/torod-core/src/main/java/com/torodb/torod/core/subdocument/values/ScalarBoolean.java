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

import com.google.common.primitives.Booleans;
import com.torodb.torod.core.subdocument.ScalarType;
import javax.annotation.concurrent.Immutable;

/**
 *
 */
@Immutable
public class ScalarBoolean extends ScalarValue<Boolean> {

    private static final long serialVersionUID = -4331050302245400288L;

    public static final ScalarBoolean TRUE = new ScalarBoolean(true);
    public static final ScalarBoolean FALSE = new ScalarBoolean(false);

    private final boolean value;

    private ScalarBoolean(boolean value) {
        this.value = value;
    }

    public static ScalarBoolean from(boolean value) {
        return value ? TRUE : FALSE;
    }

    public boolean getPrimitiveValue() {
        return value;
    }

    @Override
    public Boolean getValue() {
        return value;
    }

    @Override
    public Class<? extends Boolean> getValueClass() {
        return Boolean.class;
    }

    @Override
    public ScalarType getType() {
        return ScalarType.BOOLEAN;
    }

    @Override
    public String toString() {
        return getValue().toString();
    }

    /**
     * The hashCode of a ScalarBoolean the same as {@link Boolean#hashCode() }
     * applied to its value.
     *
     * @return
     */
    @Override
    public int hashCode() {
        return Booleans.hashCode(getPrimitiveValue());
    }

    /**
     * Two ScalarBoolean are equal if their primitive values are equal.
     * @param obj
     * @return
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof ScalarBoolean)) {
            return false;
        }
        return this.getPrimitiveValue() == ((ScalarBoolean) obj).getPrimitiveValue();
    }

    @Override
    public <Result, Arg> Result accept(ScalarValueVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this,arg);
    }
}
