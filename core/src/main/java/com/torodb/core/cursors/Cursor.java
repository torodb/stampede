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
 * along with core. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.core.cursors;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nonnull;

/**
 *
 */
public interface Cursor<E> extends AutoCloseable, Iterator<E> {

    /**
     * Gets up to maxSize elements from the current position.
     *
     * @param maxSize
     * @return
     */
    default @Nonnull List<E> getNextBatch(int maxSize) {
        List<E> elements = new ArrayList<>(maxSize);

        for (int index = 0; index < maxSize && hasNext(); index++) {
            elements.add(next());
        }

        return elements;
    }

    /**
     * Gets remaining elements from the current position.
     *
     * @return 
     */
    default @Nonnull List<E> getRemaining() {
        List<E> elements = new ArrayList<>();

        while (hasNext()) {
            elements.add(next());
        }
        return elements;
    }

    default @Nonnull <O> Cursor<O> transform(Function<E, O> transformation) {
        return new DelegateCursor<>(this, transformation);
    }

    /**
     * Releases this {@code Cursor} object's resources immediately.
     */
    @Override
    void close();

}
