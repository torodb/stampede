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

package com.torodb.mongodb.repl.filters;

import com.torodb.mongodb.filters.DatabaseFilter;
import com.torodb.mongodb.filters.IndexFilter;
import com.torodb.mongodb.filters.NamespaceFilter;

/**
 * A {@link ReplicationFilters} that delegates on another.
 *
 * It is useful to create new replication filters that override a specific method.
 */
public class DelegateReplicationFilters implements ReplicationFilters {

  private final ReplicationFilters delegate;

  public DelegateReplicationFilters(ReplicationFilters delegate) {
    this.delegate = delegate;
  }

  @Override
  public DatabaseFilter getDatabaseFilter() {
    return delegate.getDatabaseFilter();
  }

  @Override
  public NamespaceFilter getNamespaceFilter() {
    return delegate.getNamespaceFilter();
  }

  @Override
  public IndexFilter getIndexFilter() {
    return delegate.getIndexFilter();
  }

}
