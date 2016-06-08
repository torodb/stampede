/*
 * Copyright (C) 2015 José Paumard
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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