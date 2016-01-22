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
import com.torodb.torod.core.subdocument.SubDocType;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 *
 */
@Immutable
public class DocStructure implements StructureElement {
    private static final long serialVersionUID = 1L;

    @Nonnull
    private final SubDocType type;
    private final int index;
    @Nonnull
    private final Map<String, StructureElement> elements;

    public DocStructure(
            SubDocType type, 
            int index, 
            Map<String, StructureElement> elements
    ) {
        this.type = type;
        this.index = index;
        this.elements = Collections.unmodifiableMap(elements);
    }

    public SubDocType getType() {
        return type;
    }

    public int getIndex() {
        return index;
    }

    public Map<String, StructureElement> getElements() {
        return elements;
    }

    @Override
    public String toString() {
        return "DocStructure{" + "type=" + type + ", index=" + index + ", elements=" + elements + '}';
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 37 * hash + this.type.hashCode();
        hash = 37 * hash + this.index;
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final DocStructure other = (DocStructure) obj;
        if (!this.type.equals(other.type)) {
            return false;
        }
        if (this.index != other.index) {
            return false;
        }
        if (!this.elements.equals(other.elements)) {
            return false;
        }
        return true;
    }


    @Override
    public <Result, Arg> Result accept(StructureElementVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this, arg);
    }

    public static class Builder {

        private SubDocType type;
        private int index;
        private Map<String, StructureElement> elements;

        public Builder() {
            this.elements = Maps.newHashMap();
        }

        public Builder setType(SubDocType type) {
            this.type = type;
            return this;
        }
        
        public Builder setIndex(int index) {
            this.index = index;
            return this;
        }

        public Builder add(String key, @Nonnull StructureElement element) {
            Preconditions.checkArgument(element != null, "Attempt to add a null structure with key "+key+". Structure elements cannot be null");
            Preconditions.checkArgument(!elements.containsKey(key), "Element "+key+" is already contained in this builder");
            elements.put(key, element);
            return this;
        }

        public Map<String, StructureElement> getElements() {
            return Collections.unmodifiableMap(elements);
        }

        public DocStructure built() {
            if (type == null) {
                throw new IllegalArgumentException("Type cannot be null");
            }
            
            DocStructure result = new DocStructure(
                    type, 
                    index, 
                    Maps.newHashMap(elements)
            );
            elements = null;
            
            return result;
        }
    }

}
