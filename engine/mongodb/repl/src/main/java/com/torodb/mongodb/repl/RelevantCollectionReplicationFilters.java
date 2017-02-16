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

import com.torodb.mongodb.filters.FilterResult;
import com.torodb.mongodb.filters.NamespaceFilter;
import com.torodb.mongodb.language.Namespace;
import com.torodb.mongodb.utils.NamespaceUtil;

/**
 * A {@link ReplicationFilters} that is fulfilled by namespaces that are relevant to ToroDB.
 */
public class RelevantCollectionReplicationFilters extends DelegateReplicationFilters {

  RelevantCollectionReplicationFilters() {
    super(ReplicationFilters.allowAll());
  }

  @Override
  public NamespaceFilter getNamespaceFilter() {
    return this::filterCollection;
  }

  private FilterResult<Namespace> filterCollection(String db, String col) {
    if (NamespaceUtil.isSystem(col)) {
      if (NamespaceUtil.isIndexesMetaCollection(col)) {
        return FilterResult.success();
      }
      return FilterResult.failure(col2 -> col2 + " is a system collection");
    }
    return FilterResult.success();
  }
}
