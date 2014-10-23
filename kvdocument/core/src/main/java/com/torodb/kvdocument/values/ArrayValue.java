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

package com.torodb.kvdocument.values;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.torodb.kvdocument.types.ArrayType;
import com.torodb.kvdocument.types.DocType;
import com.torodb.kvdocument.types.GenericType;
import com.torodb.kvdocument.types.ObjectType;
import java.util.*;
import javax.annotation.Nonnull;

/**
 *
 */
public class ArrayValue implements DocValue, List<DocValue> {

    private final DocType elementType;
    private final List<DocValue> value;

    public ArrayValue(DocType elementType, List<DocValue> value) {
        this.elementType = elementType;
        this.value = Collections.unmodifiableList(value);
    }

    public DocType getElementType() {
        return elementType;
    }

    @Override
    public List<DocValue> getValue() {
        return value;
    }

    @Override
    public ArrayType getType() {
        return new ArrayType(elementType);
    }

    @Override
    public <Result, Arg> Result accept(DocValueVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this, arg);
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
    public Iterator<DocValue> iterator() {
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
    public boolean add(DocValue e) {
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
    public boolean addAll(Collection<? extends DocValue> c) {
        return value.addAll(c);
    }

    @Override
    public boolean addAll(int index, Collection<? extends DocValue> c) {
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
    public boolean equals(Object o) {
        return value.equals(o);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public DocValue get(int index) {
        return value.get(index);
    }

    @Override
    public DocValue set(int index, DocValue element) {
        return value.set(index, element);
    }

    @Override
    public void add(int index, DocValue element) {
        value.add(index, element);
    }

    @Override
    public DocValue remove(int index) {
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
    public ListIterator<DocValue> listIterator() {
        return value.listIterator();
    }

    @Override
    public ListIterator<DocValue> listIterator(int index) {
        return value.listIterator(index);
    }

    @Override
    public List<DocValue> subList(int fromIndex, int toIndex) {
        return value.subList(fromIndex, toIndex);
    }

    public static class Builder {

        private boolean built;
        private DocType elementType;
        private List<DocValue> values;
        private final Map<Integer, ArrayValue.Builder> subArrayBuilders
                = Maps.newHashMap();
        private final Map<Integer, ObjectValue.Builder> subObjectBuilders
                = Maps.newHashMap();

        public Builder() {
            built = false;
            values = Lists.newArrayList();
        }

        public Builder(int expectedSize) {
            built = false;
            values = Lists.newArrayListWithExpectedSize(expectedSize);
        }

        public static ArrayValue.Builder from(ArrayValue original) {
            ArrayValue.Builder result = new ArrayValue.Builder();
            result.copy(original);

            return result;
        }

        public boolean contains(int key) {
            return isValue(key)
                    || isArrayBuilder(key)
                    || isObjectBuilder(key);
        }

        public boolean isValue(int index) {
            return values.get(index) != null;
        }

        @Nonnull
        public DocValue getValue(int index) {
            DocValue result = values.get(index);
            if (result == null) {
                throw new IllegalArgumentException(
                        "There is no value associated to '" + index + "' key");
            }
            return result;
        }

        public boolean isArrayBuilder(int index) {
            return subArrayBuilders.containsKey(index);
        }

        @Nonnull
        public ArrayValue.Builder getArrayBuilder(int index) {
            ArrayValue.Builder result = subArrayBuilders.get(index);
            if (result == null) {
                throw new IllegalArgumentException(
                        "There is no array builder associated to '" + index + "' key");
            }
            return result;
        }

        public boolean isObjectBuilder(int index) {
            return subObjectBuilders.containsKey(index);
        }

        @Nonnull
        public ObjectValue.Builder getObjectBuilder(int index) {
            ObjectValue.Builder result = subObjectBuilders.get(index);
            if (result == null) {
                throw new IllegalArgumentException(
                        "There is no object builder associated to '" + index + "' key");
            }
            return result;
        }

        private void setElement(int index, DocValue element) {
            prepareSize(index);
            values.set(index, element);
            subObjectBuilders.remove(index);
            subArrayBuilders.remove(index);
        }

        private void setArrayBuilder(int index, ArrayValue.Builder builder) {
            prepareSize(index);
            values.set(index, null);
            subObjectBuilders.remove(index);
            subArrayBuilders.put(index, builder);
        }

        private void setObjectBuilder(int index, ObjectValue.Builder builder) {
            prepareSize(index);
            values.set(index, null);
            subObjectBuilders.put(index, builder);
            subArrayBuilders.remove(index);
        }

        public ArrayValue.Builder newArray(int index) {
            checkNewBuild();

            ArrayValue.Builder result = new Builder();
            setArrayBuilder(index, result);

            return result;
        }

        public ObjectValue.Builder newObject(int index) {
            checkNewBuild();

            ObjectValue.Builder result = new ObjectValue.Builder();
            setObjectBuilder(index, result);

            return result;
        }

        public void setElementType(DocType newElementType) {
            checkNewBuild();

            DocType realElementType = calculateElementType();
            if (!newElementType.equals(GenericType.INSTANCE)
                    && !newElementType.equals(realElementType)) {
                throw new IllegalArgumentException(
                        "The new element type is not consistent with values "
                        + "contained in the builder."
                );
            }
            this.elementType = newElementType;
        }

        public boolean unset(int index) {
            if (values.size() >= index) {
                return false;
            }
            setElement(index, NullValue.INSTANCE);
            return true;
        }

        public Builder add(DocValue newVal) {
            checkNewBuild();

            if (elementType != null
                    && !elementType.equals(GenericType.INSTANCE)
                    && !newVal.getType().equals(elementType)) {
                throw new IllegalArgumentException(
                        "Element " + newVal + " does not conform to the element "
                        + "type of the array (" + elementType + ")"
                );
            }
            values.add(newVal);

            return this;
        }

        public Builder setValue(int index, DocValue newValue) {
            checkNewBuild();

            if (elementType != null
                    && !elementType.equals(GenericType.INSTANCE)
                    && !newValue.getType().equals(elementType)) {
                throw new IllegalArgumentException(
                        "Element " + newValue + " does not conform to the element "
                        + "type of the array (" + elementType + ")");
            }
            if (newValue instanceof ObjectValue) {
                newObject(index).copy((ObjectValue) newValue);
            } else {
                if (newValue instanceof ArrayValue) {
                    newArray(index).copy((ArrayValue) newValue);
                } else {
                    setElement(index, newValue);
                }
            }
            return this;
        }

        public ArrayValue build() {
            elementType = calculateElementType();
            built = true;

            for (Map.Entry<Integer, ObjectValue.Builder> objectBuilder
                    : subObjectBuilders.entrySet()) {

                DocValue oldValue
                        = values.set(
                                objectBuilder.getKey(),
                                objectBuilder.getValue().build()
                        );

                assert oldValue == null;
            }
            for (Map.Entry<Integer, Builder> arrayBuilder
                    : subArrayBuilders.entrySet()) {

                DocValue oldValue
                        = values.set(
                                arrayBuilder.getKey(),
                                arrayBuilder.getValue().build()
                        );

                assert oldValue == null;
            }
            subObjectBuilders.clear();
            subArrayBuilders.clear();

            return new ArrayValue(elementType, values);
        }

        private DocType getType(int index) {
            DocValue value = values.get(index);
            if (value != null) {
                return value.getType();
            }
            if (isObjectBuilder(index)) {
                return ObjectType.INSTANCE;
            } else if (isArrayBuilder(index)) {
                return getArrayBuilder(index).calculateElementType();
            } else {
                throw new IllegalStateException();
            }
        }

        private DocType calculateElementType() {
            DocType result = null;
            for (int i = 0; i < values.size(); i++) {
                DocType iestType = getType(i);
                if (result == null) {
                    result = iestType;
                } else {
                    if (!result.equals(iestType)) {
                        result = GenericType.INSTANCE;
                        break;
                    }
                }
            }

            return result;
        }

        void copy(ArrayValue original) {
            values.clear();
            subArrayBuilders.clear();
            subObjectBuilders.clear();

            for (int i = 0; i < original.size(); i++) {
                DocValue value = original.get(i);

                if (value instanceof ArrayValue) {
                    ArrayValue.Builder childBuilder = newArray(i);
                    childBuilder.copy((ArrayValue) value);
                } else if (value instanceof ObjectValue) {
                    ObjectValue.Builder childBuilder = newObject(i);
                    childBuilder.copy((ObjectValue) value);
                } else {
                    setValue(i, value);
                }
            }
        }

        private void checkNewBuild() {
            if (built) {
                values = Lists.newArrayList();
                built = false;
            }
        }

        private void prepareSize(int minSize) {
            for (int i = values.size(); i <= minSize; i++) {
                values.add(NullValue.INSTANCE);
            }
        }

    }
}
