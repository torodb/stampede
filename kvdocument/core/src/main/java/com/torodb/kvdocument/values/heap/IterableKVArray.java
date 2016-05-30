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

package com.torodb.kvdocument.values.heap;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.UnmodifiableIterator;
import com.torodb.kvdocument.annotations.NotMutable;
import com.torodb.kvdocument.values.KVArray;
import com.torodb.kvdocument.values.KVValue;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.ObjectStreamException;

/**
 *
 */
@SuppressFBWarnings(value = {"SE_BAD_FIELD", "SE_NO_SERIALVERSIONID"},
        justification = "writeReplace is used")
public class IterableKVArray extends KVArray {

    private static final long serialVersionUID = -1955250327304119290L;

    private final Iterable<KVValue<?>> iterable;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",
            justification = "We know this can be dangerous, but it improves the efficiency and, by"
                    + "contract, the iterable shall be immutable")
    public IterableKVArray(@NotMutable Iterable<KVValue<?>> iterable) {
        this.iterable = iterable;
    }

    @Override
    public UnmodifiableIterator<KVValue<?>> iterator() {
        return Iterators.unmodifiableIterator(iterable.iterator());
    }

    private Object writeReplace() throws ObjectStreamException {
        return new ListKVArray(Lists.newArrayList(this));
    }
}
