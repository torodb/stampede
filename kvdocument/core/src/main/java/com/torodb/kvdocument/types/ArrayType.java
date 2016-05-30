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


package com.torodb.kvdocument.types;

import javax.annotation.concurrent.Immutable;

/**
 *
 */
@Immutable
public class ArrayType implements KVType {
    private static final long serialVersionUID = 1L;
    
    private final KVType elementType;

    public ArrayType(KVType elementType) {
        this.elementType = elementType;
    }

    public KVType getElementType() {
        return elementType;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 23 * hash +
                (this.elementType != null ? this.elementType.hashCode() : 0);
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
        final ArrayType other = (ArrayType) obj;
        if (this.elementType != other.elementType &&
                (this.elementType == null ||
                !this.elementType.equals(other.elementType))) {
            return false;
        }
        return true;
    }

    @Override
    public <Result, Arg> Result accept(KVTypeVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this, arg);
    }

    @Override
    public String toString() {
        return "Array<" + elementType + '>';
    }
    
}
