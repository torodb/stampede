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

package com.torodb.torod.core.subdocument.values.heap;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.UnmodifiableIterator;
import com.torodb.kvdocument.annotations.NotMutable;
import com.torodb.torod.core.subdocument.values.ScalarArray;
import com.torodb.torod.core.subdocument.values.ScalarValue;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.ObjectStreamException;

/**
 *
 */
@SuppressFBWarnings(value = {"SE_BAD_FIELD", "SE_NO_SERIALVERSIONID"},
        justification = "writeReplace is used")
public class IterableScalarArray extends ScalarArray {

    private static final long serialVersionUID = 6758446383200925139L;

    private final Iterable<ScalarValue<?>> iterable;

    public IterableScalarArray(@NotMutable Iterable<ScalarValue<?>> iterable) {
        this.iterable = iterable;
    }

    @Override
    public UnmodifiableIterator<ScalarValue<?>> iterator() {
        return Iterators.unmodifiableIterator(iterable.iterator());
    }

    private Object writeReplace() throws ObjectStreamException {
        return new ListScalarArray(Lists.newArrayList(this));
    }
}
