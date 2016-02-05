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
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 *
 */
@Immutable
public class SubDocAttribute implements Serializable {
    private static final long serialVersionUID = 1L;

    @Nonnull
    private final String key;
    @Nonnull
    private final ScalarType type;

    public SubDocAttribute(String key, ScalarType type) {
        this.key = key;
        this.type = type;
    }

    @Nonnull
    public String getKey() {
        return key;
    }

    @Nonnull
    public ScalarType getType() {
        return type;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 67 * hash + this.key.hashCode();
        hash = 67 * hash + this.type.hashCode();
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
        if (!this.key.equals(other.key)) {
            return false;
        }
        return equalsWithSameKey(other);
    }

    boolean equalsWithSameKey(SubDocAttribute other) {
        assert other.getKey().equals(this.getKey());

        return this.type.equals(other.type);
    }

}
