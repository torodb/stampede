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

package com.torodb.torod.core.utils;

import com.google.common.base.Preconditions;
import javax.annotation.Nonnull;

/**
 * Instances of this class represents values that can be a
 * {@linkplain #getValue() real value}, {@link #isNull() null} or
 * {@linkplain #isUndecidable() undecidable}.
 * <p>
 * @param <E> The type of the represented value
 */
public class TriValuedResult<E> {

    private final boolean undecidable;
    private final E value;
    public static final TriValuedResult UNDECIDABLE = new TriValuedResult(true);
    public static final TriValuedResult NULL = new TriValuedResult(false);
    public static final TriValuedResult<Boolean> TRUE = new TriValuedResult(Boolean.TRUE);
    public static final TriValuedResult<Boolean> FALSE = new TriValuedResult(Boolean.FALSE);

    private TriValuedResult(boolean undecidable) {
        this.undecidable = undecidable;
        this.value = null;
    }

    protected TriValuedResult(@Nonnull E value) {
        this.undecidable = false;
        this.value = Preconditions.checkNotNull(value);
    }

    public static <E> TriValuedResult<E> createValue(E value) {
        return new TriValuedResult<>(value);
    }
    
    public boolean isUndecidable() {
        return undecidable;
    }

    public boolean isNull() {
        return !undecidable && value == null;
    }

    @Nonnull
    public E getValue() {
        if (undecidable) {
            throw new IllegalArgumentException("The value is not decidable");
        }
        if (value == null) {
            throw new IllegalArgumentException("The value is not defined");
        }
        return value;
    }

    @Override
    public String toString() {
        if (undecidable) {
            return "Resut: undecidable";
        }
        if (value == null) {
            return "Result: no found";
        }
        return "Result: " + value;
    }

}
