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
import java.io.Serializable;
import javax.annotation.concurrent.Immutable;

/**
 *
 */
@Immutable
public class NullValue implements Value<Serializable> {

    public static final NullValue INSTANCE = new NullValue();
    private static final long serialVersionUID = 1L;
    
    private NullValue() {}
    
    @Override
    public Serializable getValue() {
        return null;
    }

    @Override
    public BasicType getType() {
        return BasicType.NULL;
    }

    @Override
    public String toString() {
        return "null";
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof NullValue;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
    
    @Override
    public <Result, Arg> Result accept(ValueVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this, arg);
    }
}
