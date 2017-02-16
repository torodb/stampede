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

public class AndFilterResult<E> implements FilterResult<E> {
  private final FilterResult<E> r1;
  private final FilterResult<E> r2;

  public AndFilterResult(FilterResult<E> r1, FilterResult<E> r2) {
    this.r1 = r1;
    this.r2 = r2;
  }

  @Override
  public boolean isSuccessful() {
    return r1.isSuccessful() && r2.isSuccessful();
  }

  @Override
  public Optional<Function<E, String>> getReason() {
    if (!r1.isSuccessful()) {
      return r1.getReason();
    }
    if (!r2.isSuccessful()) {
      return r2.getReason();
    }
    return Optional.empty();
  }
}
