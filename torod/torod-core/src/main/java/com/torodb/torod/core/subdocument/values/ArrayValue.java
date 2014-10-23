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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.subdocument.BasicType;
import java.util.*;
import javax.annotation.concurrent.Immutable;

/**
 * Represents an array of values.
 * 
 * A subdocument array cannot contain sub document elements because sub documents are not values!
 * 
 */
@Immutable
public class ArrayValue implements Value<ImmutableList<Value<?>>>, List<Value<?>> {
    private static final long serialVersionUID = 1L;

    private final ImmutableList<Value<?>> value;

    private ArrayValue(List<Value<?>> value) {
        this.value = ImmutableList.copyOf(value);
    }

    @Override
    public ImmutableList<Value<?>> getValue() {
        return value;
    }

    @Override
    public BasicType getType() {
        return BasicType.ARRAY;
    }
    
    public Value<?> resolve(List<? extends AttributeReference.Key> keys) {
        return resolve(keys, 0);
    }
    
    private Value<?> resolve(List<? extends AttributeReference.Key> keys, int depth) {
        AttributeReference.Key localKey = keys.get(depth);
        if (!(localKey instanceof AttributeReference.ArrayKey)) {
            throw new IllegalArgumentException("Recived keys (" +keys +") contains key '" + localKey+"' which is not"
                    + "an array key ("+localKey+" is at depth "+depth+")");
        }
        Value<?> referencedValue = value.get(((AttributeReference.ArrayKey) localKey).getIndex());
        if (depth == keys.size()) {
            return referencedValue;
        }
        else {
            if (!(referencedValue instanceof ArrayValue)) {
                throw new IllegalArgumentException(keys + " is not valid because element at "+depth+
                        " ("+keys.subList(0, depth)+") is a "+referencedValue.getType()+" while an array was expected");
            }
            return ((ArrayValue) referencedValue).resolve(keys, depth + 1);
        }
    }

    @Override
    public String toString() {
        return value.toString();
    }

    @Override
    public <Result, Arg> Result accept(ValueVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this, arg);
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 29 * hash + (this.value != null ? this.value.hashCode() : 0);
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
        final ArrayValue other = (ArrayValue) obj;
        if (this.value != other.value && (this.value == null || !this.value.equals(other.value))) {
            return false;
        }
        return true;
    }

    @Override
    public int size() {
        return value.size();
    }

    @Override
    public boolean isEmpty() {
        return value.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return value.contains(o);
    }

    @Override
    public Iterator<Value<?>> iterator() {
        return value.iterator();
    }

    @Override
    public Object[] toArray() {
        return value.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return value.toArray(a);
    }

    @Override
    public boolean add(Value<?> e) {
        return value.add(e);
    }

    @Override
    public boolean remove(Object o) {
        return value.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return value.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends Value<?>> c) {
        return value.addAll(c);
    }

    @Override
    public boolean addAll(int index, Collection<? extends Value<?>> c) {
        return value.addAll(index, c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return value.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return value.retainAll(c);
    }

    @Override
    public void clear() {
        value.clear();
    }

    @Override
    public Value<?> get(int index) {
        return value.get(index);
    }

    @Override
    public Value<?> set(int index, Value<?> element) {
        return value.set(index, element);
    }

    @Override
    public void add(int index, Value<?> element) {
        value.add(index, element);
    }

    @Override
    public Value<?> remove(int index) {
        return value.remove(index);
    }

    @Override
    public int indexOf(Object o) {
        return value.indexOf(o);
    }

    @Override
    public int lastIndexOf(Object o) {
        return value.lastIndexOf(o);
    }

    @Override
    public ListIterator<Value<?>> listIterator() {
        return value.listIterator();
    }

    @Override
    public ListIterator<Value<?>> listIterator(int index) {
        return value.listIterator(index);
    }

    @Override
    public List<Value<?>> subList(int fromIndex, int toIndex) {
        return value.subList(fromIndex, toIndex);
    }
    
    public static class Builder {

        private final ArrayList<Value<?>> values = Lists.newArrayList();;

        public Builder add(Value value) {
            values.add(value);
            return this;
        }
        
        public Builder addAll(Iterable<Value<?>> values) {
            for (Value value1 : values) {
                this.values.add(value1);
            }
            return this;
        }
        
        public ArrayValue build() {
            return new ArrayValue(values);
        }
    }

}
