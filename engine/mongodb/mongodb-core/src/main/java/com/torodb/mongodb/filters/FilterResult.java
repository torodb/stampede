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


import org.apache.logging.log4j.util.Supplier;

import java.util.Optional;
import java.util.function.Function;

/**
 * The result of a replication filter.
 */
public interface FilterResult<E> {

  public static <E> FilterResult<E> success() {
    return new DefaultFilterResult<>(true, Optional.empty());
  }

  public static <E> FilterResult<E> failure(Function<E, String> reason) {
    return new DefaultFilterResult<>(false, Optional.of(reason));
  }

  /**
   * If the element fulfilled the filter.
   */
  public boolean isSuccessful();

  /**
   * An optional function that, if present, indicates why the element does not fulfill the filter.
   */
  public Optional<Function<E, String>> getReason();

  public default Supplier<String> getReasonAsSupplier(E element) {
    return () -> {
      Optional<Function<E, String>> reason = getReason();
      if (reason.isPresent()) {
        return reason.get().apply(element);
      } else {
        return "unknown";
      }
    };
  }

  public default FilterResult<E> and(FilterResult<E> other) {
    return new AndFilterResult<>(this, other);
  }

  /**
   * Returns a new {@link FilterResult} that maps the reason to a new function.
   * @param trans a function that maps from this reason to the new one.
   */
  public default <O> FilterResult<O> map(Function<Function<E, String>, Function<O, String>> trans) {
    return new MapFilterResult<>(this, trans);
  }

}
