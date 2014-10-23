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
import javax.annotation.concurrent.Immutable;

/**
 *
 */
@Immutable
public class SubDocAttribute implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String key;
    private final BasicType type;

    public SubDocAttribute(String key, BasicType type) {
        this.key = key;
        this.type = type;
    }

    public String getKey() {
        return key;
    }

    public BasicType getType() {
        return type;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 67 * hash + (this.key != null ? this.key.hashCode() : 0);
        hash = 67 * hash + (this.type != null ? this.type.hashCode() : 0);
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
        final SubDocAttribute other = (SubDocAttribute) obj;
        if ((this.key == null) ? (other.key != null) : !this.key.equals(other.key)) {
            return false;
        }
        if (this.type != other.type && (this.type == null || !this.type.equals(other.type))) {
            return false;
        }
        return true;
    }

}
