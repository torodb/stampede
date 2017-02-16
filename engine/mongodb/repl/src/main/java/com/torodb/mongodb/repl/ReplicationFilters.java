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

package com.torodb.mongodb.repl;


import com.torodb.mongodb.filters.DatabaseFilter;
import com.torodb.mongodb.filters.FilterResult;
import com.torodb.mongodb.filters.IndexFilter;
import com.torodb.mongodb.filters.NamespaceFilter;

import java.util.Objects;


public interface ReplicationFilters {
  public DatabaseFilter getDatabaseFilter();

  public NamespaceFilter getNamespaceFilter();

  public IndexFilter getIndexFilter();

  public default ReplicationFilters and(ReplicationFilters other) {
    Objects.requireNonNull(other);
    final ReplicationFilters self = this;
    return new ReplicationFilters() {
      @Override
      public DatabaseFilter getDatabaseFilter() {
        return self.getDatabaseFilter().and(other.getDatabaseFilter());
      }

      @Override
      public NamespaceFilter getNamespaceFilter() {
        return self.getNamespaceFilter().and(other.getNamespaceFilter());
      }

      @Override
      public IndexFilter getIndexFilter() {
        return self.getIndexFilter().and(other.getIndexFilter());
      }
    };
  }

  /**
   * Returns a {@link ReplicationFilters} that always return true.
   */
  public static ReplicationFilters allowAll() {
    return new ReplicationFilters() {
      @Override
      public DatabaseFilter getDatabaseFilter() {
        return this::filter;
      }

      @Override
      public NamespaceFilter getNamespaceFilter() {
        return (db, col) -> FilterResult.success();
      }

      @Override
      public IndexFilter getIndexFilter() {
        return this::filter;
      }

      private <E> FilterResult<E> filter(E e) {
        return FilterResult.success();
      }
    };
  }
}
