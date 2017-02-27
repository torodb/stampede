/*
 * ToroDB
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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.mongodb.filters;

import java.util.Objects;
import java.util.function.Function;

/**
 * A filter that instead of a boolean, returns a {@link FilterResult}, so it can be used to show
 * the reason why the element does not fulfill the filter.
 */
@FunctionalInterface
public interface Filter<E> extends Function<E, FilterResult<E>> {

  default boolean filter(E element) {
    return this.apply(element).isSuccessful();
  }

  public default Filter<E> and(Filter<E> other) {
    Objects.requireNonNull(other);
    return (e) -> apply(e).and(other.apply(e));
  }

}
