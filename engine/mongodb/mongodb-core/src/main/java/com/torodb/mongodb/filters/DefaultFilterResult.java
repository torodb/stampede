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


import java.util.Optional;
import java.util.function.Function;

/**
 * The result of a replication filter.
 */
class DefaultFilterResult<E> implements FilterResult<E> {
  private final boolean successful;
  private final Optional<Function<E, String>> reason;

  DefaultFilterResult(boolean success, Optional<Function<E, String>> reason) {
    this.successful = success;
    this.reason = reason;
  }

  /**
   * If the element fulfilled the filter.
   */
  public boolean isSuccessful() {
    return successful;
  }

  /**
   * An optional function that, if present, indicates why the element does not fulfill the filter.
   */
  public Optional<Function<E, String>> getReason() {
    return reason;
  }
}
