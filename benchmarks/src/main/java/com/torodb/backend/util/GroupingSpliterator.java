
/*
 * ToroDB - ToroDB-poc: Benchmarks
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.torodb.backend.util;

import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * See the documentation and patterns to be used in this class in the {@link MoreSpliterators} factory class.
 *
 * https://github.com/JosePaumard/more-spliterators
 * 
 * @author José
 */
public class GroupingSpliterator<E> implements Spliterator<Stream<E>> {

    private final long grouping;
    private final Spliterator<E> spliterator;
    private Stream.Builder<E> builder = Stream.builder();
    private boolean firstGroup = true;

    public static <E> GroupingSpliterator<E> of(Spliterator<E> spliterator, long grouping) {
        Objects.requireNonNull(spliterator);
        if (grouping < 2) {
            throw new IllegalArgumentException("Why would you build a grouping spliterator with a grouping factor of less than 2?");
        }
        if ((spliterator.characteristics() & Spliterator.ORDERED) == 0) {
            throw new IllegalArgumentException("Why would you build a grouping spliterator on a non-ordered spliterator?");
        }

        return new GroupingSpliterator<>(spliterator, grouping);
    }

    private GroupingSpliterator(Spliterator<E> spliterator, long grouping) {
        this.spliterator = spliterator;
        this.grouping = grouping;
    }

    @Override
    public boolean tryAdvance(Consumer<? super Stream<E>> action) {
        boolean moreElements = true;
        if (firstGroup) {
            moreElements = spliterator.tryAdvance(builder::add);
            firstGroup = false;
        }
        if (!moreElements) {
            action.accept(builder.build());
            return false;
        }
        for (int i = 1; i < grouping && moreElements; i++) {
            if (!spliterator.tryAdvance(builder::add)) {
                moreElements = false;
            }
        }
        Stream<E> subStream = builder.build();
        action.accept(subStream);
        if (moreElements) {
            builder = Stream.builder();
            moreElements = spliterator.tryAdvance(builder::add);
        }

        return moreElements;
    }

    @Override
    public Spliterator<Stream<E>> trySplit() {
        Spliterator<E> splitSpliterator = this.spliterator.trySplit();
        return splitSpliterator == null ? null : new GroupingSpliterator<>(splitSpliterator, grouping);
    }

    @Override
    public long estimateSize() {
        long estimateSize = spliterator.estimateSize();
        return estimateSize == Long.MAX_VALUE ? Long.MAX_VALUE : estimateSize / grouping;
    }

    @Override
    public int characteristics() {
        // this spliterator is already ordered
        return spliterator.characteristics();
    }
}