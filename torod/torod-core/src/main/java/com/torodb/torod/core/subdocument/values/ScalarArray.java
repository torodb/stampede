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

import com.google.common.collect.Iterables;
import com.google.common.hash.Hashing;
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.subdocument.ScalarType;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * Represents an array of values.
 * 
 * A subdocument array cannot contain sub document elements because sub documents are not values!
 * 
 */
@Immutable
public abstract class ScalarArray extends ScalarValue<ScalarArray> implements Iterable<ScalarValue<?>> {

    private static final long serialVersionUID = 7676030304713277160L;

    @Override
    public ScalarArray getValue() {
        return this;
    }

    @Override
    public ScalarType getType() {
        return ScalarType.ARRAY;
    }

    @Override
    public Class<? extends ScalarArray> getValueClass() {
        return getClass();
    }
    
    public ScalarValue<?> resolve(List<? extends AttributeReference.Key> keys) {
        return resolve(keys, 0);
    }

    @Override
    public <Result, Arg> Result accept(ScalarValueVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this, arg);
    }

    @Nonnull
    public ScalarValue<?> get(int index) throws IndexOutOfBoundsException {
        return Iterables.get(this, index);
    }

    public boolean contains(ScalarValue<?> element) {
        return Iterables.contains(this, element);
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public int size() {
        return Iterables.size(this);
    }

    @Override
    public String toString() {
        return Iterables.toString(this);
    }

    /**
     * @return
     *         {@linkplain Hashing#goodFastHash(int) Hashing.goodFastHash(32).newHasher().putInt(size()).putInt(getElementType().hashCode()).hash().asInt();}
     */
    @Override
    public int hashCode() {
        return Hashing.goodFastHash(32).hashInt(size()).asInt();
    }

    /**
     * Implementations can override this method to optimize the equality check.
     *
     * Some implementations can optimize the equality check using specific
     * improvements. For example an implementation that uses an ArrayList can
     * check if another ScalarArray is different by checking their sizes, which is
     * O(1) on ArrayLists instead of O(n) in general implementations.
     *
     * If this method return true, {@link Iterables#elementsEqual(java.lang.Iterable, java.lang.Iterable) }
     * will be called to check if the content of both ScalarArray are equal. If
     * it return false, {@link #equals(java.lang.Object) } will return false
     * without iterating over the arrays.
     * @param other
     * @return
     */
    protected boolean equalsOptimization(ScalarArray other) {
        return true;
    }

    /**
     * Two ArrayValues values are equal if their contains equal elements in the
     * same position.
     *
     * An easy way to implement that is to delegate on
     * {@link Iterators#elementsEqual(java.lang.Iterator, java.lang.Iterator) }
     *
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
        if (!(obj instanceof ScalarArray)) {
            return false;
        }
        ScalarArray other = (ScalarArray) obj;
        if (!equalsOptimization(other)) {
            return false;
        }
        return Iterables.elementsEqual(other, this);
    }

    private ScalarValue<?> resolve(List<? extends AttributeReference.Key> keys, int depth) {
        AttributeReference.Key localKey = keys.get(depth);
        if (!(localKey instanceof AttributeReference.ArrayKey)) {
            throw new IllegalArgumentException("Recived keys (" +keys +") contains key '" + localKey+"' which is not"
                    + "an array key ("+localKey+" is at depth "+depth+")");
        }
        ScalarValue<?> referencedValue = get(((AttributeReference.ArrayKey) localKey).getIndex());
        if (depth == keys.size()) {
            return referencedValue;
        }
        else {
            if (!(referencedValue instanceof ScalarArray)) {
                throw new IllegalArgumentException(keys + " is not valid because element at "+depth+
                        " ("+keys.subList(0, depth)+") is a "+referencedValue.getType()+" while an array was expected");
            }
            return ((ScalarArray) referencedValue).resolve(keys, depth + 1);
        }
    }
}
