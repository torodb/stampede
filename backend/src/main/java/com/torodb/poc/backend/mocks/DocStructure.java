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

package com.torodb.poc.backend.mocks;

import java.util.Collections;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 *
 */
@Immutable
public class DocStructure {
    private static final long serialVersionUID = 1L;

    @Nonnull
    private final PathDocStructure rootStructure;
    @Nonnull
    private final Map<String, PathDocStructure> childs;

    public DocStructure(
            PathDocStructure rootStructure, 
            Map<String, PathDocStructure> elements
    ) {
        this.rootStructure = rootStructure;
        this.childs = Collections.unmodifiableMap(elements);
    }

    public PathDocStructure getRootStructureType() {
        return rootStructure;
    }

    public Map<String, PathDocStructure> getChilds() {
        return childs;
    }

    @Override
    public String toString() {
        return "DocStructure{" + "type=" + rootStructure + ", elements=" + childs + '}';
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 37 * hash + this.rootStructure.hashCode();
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
        if (!this.rootStructure.equals(other.rootStructure)) {
            return false;
        }
        if (!this.childs.equals(other.childs)) {
            return false;
        }
        return true;
    }


    public static class Builder {

        private PathDocStructure rootStructure;
        private Map<String, PathDocStructure> childs;

        public Builder() {
            this.childs = Maps.newHashMap();
        }

        public Builder setRootStructure(PathDocStructure rootStructure) {
            this.rootStructure = rootStructure;
            return this;
        }

        public Builder add(String key, @Nonnull PathDocStructure element) {
            Preconditions.checkArgument(element != null, "Attempt to add a null structure with key "+key+". Structure elements cannot be null");
            Preconditions.checkArgument(!childs.containsKey(key), "Element "+key+" is already contained in this builder");
            childs.put(key, element);
            return this;
        }

        public Map<String, PathDocStructure> getElements() {
            return Collections.unmodifiableMap(childs);
        }

        public DocStructure built() {
            if (rootStructure == null) {
                throw new IllegalArgumentException("Root structure cannot be null");
            }
            
            DocStructure result = new DocStructure(
                    rootStructure, 
                    Maps.newHashMap(childs)
            );
            childs = null;
            
            return result;
        }
    }

}
