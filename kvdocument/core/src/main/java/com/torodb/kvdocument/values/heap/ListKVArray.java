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

package com.torodb.kvdocument.values.heap;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import com.torodb.kvdocument.annotations.NotMutable;
import com.torodb.kvdocument.values.KVArray;
import com.torodb.kvdocument.values.KVValue;
import java.util.List;
import javax.annotation.Nonnull;

/**
 *
 */
public class ListKVArray extends KVArray {

    private static final long serialVersionUID = -5242307037136472681L;

    private final List<KVValue<?>> list;

    public ListKVArray(@NotMutable List<KVValue<?>> list) {
        this.list = list;
    }

    @Override
    public UnmodifiableIterator<KVValue<?>> iterator() {
        return Iterators.unmodifiableIterator(list.iterator());
    }

    @Override
    protected boolean equalsOptimization(@Nonnull KVArray other) {
        if (other instanceof ListKVArray) {
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
    public boolean contains(KVValue<?> element) {
        return list.contains(element);
    }

    @Override
    public KVValue<?> get(int index) throws IndexOutOfBoundsException {
        return list.get(index);
    }
}
