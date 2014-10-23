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


package com.torodb.torod.core.subdocument.structure;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 *
 */
public class ArrayStructure implements StructureElement {
    private static final long serialVersionUID = 1L;
    
    private final Map<Integer, StructureElement> structureElements;

    private ArrayStructure(Map<Integer, StructureElement> structureElements) {
        this.structureElements = Collections.unmodifiableMap(structureElements);
    }
    
    @Nullable
    public StructureElement get(int index) {
        return structureElements.get(index);
    }
    
    @Nonnull
    public Map<Integer, StructureElement> getElements() {
        return structureElements;
    }

    @Override
    public <Result, Arg> Result accept(StructureElementVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this, arg);
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 97 * hash + (this.structureElements != null ? this.structureElements.hashCode() : 0);
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
        final ArrayStructure other = (ArrayStructure) obj;
        if (this.structureElements != other.structureElements && (this.structureElements == null || !this.structureElements.equals(other.structureElements))) {
            return false;
        }
        return true;
    }
    
    public static class Builder {
        private Map<Integer, StructureElement> structureElements = Maps.newHashMap();

        public Builder add(@Nonnegative int index, StructureElement element) {
            Preconditions.checkArgument(index >= 0, "Index must be non negative");
            structureElements.put(index, element);
            return this;
        }
        
        public ArrayStructure built() {
            ArrayStructure arrayStructure = new ArrayStructure(structureElements);
            structureElements = null;
            return arrayStructure;
        }
        
    } 
}
