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

public class MapFilterResult<E1, E2> implements FilterResult<E2> {
  private final FilterResult<E1> delegate;
  private final Function<Function<E1, String>, Function<E2, String>> transformation;

  public MapFilterResult(FilterResult<E1> delegate,
      Function<Function<E1, String>, Function<E2, String>> transformation) {
    this.delegate = delegate;
    this.transformation = transformation;
  }
  
  @Override
  public boolean isSuccessful() {
    return delegate.isSuccessful();
  }

  @Override
  public Optional<Function<E2, String>> getReason() {
    return delegate.getReason()
        .map(transformation::apply);
  }

}
