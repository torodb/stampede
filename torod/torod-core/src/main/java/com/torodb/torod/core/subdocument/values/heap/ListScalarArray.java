/*
 * This file is part of ToroDB.
 *
 * ToroDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ToroDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with kvdocument-core. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.torod.core.subdocument.values.heap;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import com.torodb.kvdocument.annotations.NotMutable;
import com.torodb.torod.core.subdocument.values.ScalarArray;
import com.torodb.torod.core.subdocument.values.ScalarValue;
import java.util.List;

/**
 *
 */
public class ListScalarArray extends ScalarArray {

    private static final long serialVersionUID = -871142300737937190L;

    private final List<ScalarValue<?>> list;

    public ListScalarArray(@NotMutable List<ScalarValue<?>> list) {
        this.list = list;
    }

    @Override
    public UnmodifiableIterator<ScalarValue<?>> iterator() {
        return Iterators.unmodifiableIterator(list.iterator());
    }

    @Override
    protected boolean equalsOptimization(ScalarArray other) {
        if (other instanceof ListScalarArray) {
            return this.size() == other.size();
        }
        return true;
    }

    @Override
    public int size() {
        return list.size();
    }

    @Override
    public boolean isEmpty() {
        return list.isEmpty();
    }

    @Override
    public boolean contains(ScalarValue<?> element) {
        return list.contains(element);
    }

    @Override
    public ScalarValue<?> get(int index) throws IndexOutOfBoundsException {
        return list.get(index);
    }
}
