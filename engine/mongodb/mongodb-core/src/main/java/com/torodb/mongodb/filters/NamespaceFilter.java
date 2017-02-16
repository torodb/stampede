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

import com.torodb.mongodb.language.Namespace;

import java.util.Objects;

@FunctionalInterface
public interface NamespaceFilter extends Filter<Namespace> {

  public FilterResult<Namespace> apply(String db, String col);

  @Override
  public default FilterResult<Namespace> apply(Namespace ns) {
    return apply(ns.getDatabase(), ns.getCollection());
  }

  public default boolean filter(String db, String col) {
    return apply(db, col).isSuccessful();
  }

  @Override
  public default NamespaceFilter and(Filter<Namespace> other) {
    Objects.requireNonNull(other);
    NamespaceFilter self = this;
    return new NamespaceFilter() {
      @Override
      public FilterResult<Namespace> apply(String db, String col) {
        Namespace ns = new Namespace(db, col);
        return self.apply(ns).and(other.apply(ns));
      }

      @Override
      public FilterResult<Namespace> apply(Namespace ns) {
        return self.apply(ns).and(other.apply(ns));
      }
    };
  }

  public default NamespaceFilter and(NamespaceFilter other) {
    return (db, col) -> this.apply(db, col).and(other.apply(db, col));
  }

}
