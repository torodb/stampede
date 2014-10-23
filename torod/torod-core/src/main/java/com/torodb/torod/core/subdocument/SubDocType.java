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

package com.torodb.torod.core.subdocument;

import java.io.Serializable;
import java.util.*;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 *
 */
@Immutable
public class SubDocType implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Map<String, SubDocAttribute> attributes;

    private SubDocType(Map<String, SubDocAttribute> attributes) {
        this.attributes = Collections.unmodifiableMap(attributes);
    }

    public Collection<SubDocAttribute> getAttributes() {
        return attributes.values();
    }

    public Set<String> getAttributeKeys() {
        return attributes.keySet();
    }
    
    public SubDocAttribute getAttribute(String key) {
        return attributes.get(key);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('(');
        for (Map.Entry<String, SubDocAttribute> entry : attributes.entrySet()) {
            sb
                    .append(entry.getKey())
                    .append(" : ")
                    .append(entry.getValue().getType())
                    .append(", ");
        }
        if (!attributes.isEmpty()) {
            sb.delete(sb.length() - 2, sb.length());
        }
        sb.append(')');
        
        return sb.toString();
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 79 * hash + (this.attributes != null ? this.attributes.hashCode() : 0);
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
        final SubDocType other = (SubDocType) obj;
        if (this.attributes != other.attributes && (this.attributes == null || !this.attributes.equals(other.attributes))) {
            return false;
        }
        return true;
    }

    public static class Builder {

        private final Map<String, SubDocAttribute> attributes = new HashMap<String, SubDocAttribute>();
        private boolean built;

        public Builder add(SubDocAttribute attribute) {
            if (built) {
                throw new IllegalStateException("This buider has already been used to build a previous type");
            }
            if (attributes.containsKey(attribute.getKey())) {
                throw new IllegalArgumentException("There is another attribute with key " + attribute.getKey());
            }
            attributes.put(attribute.getKey(), attribute);

            return this;
        }

        public SubDocType build() {
            built = true;
            return new SubDocType(attributes);
        }
    }

}
