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

import com.eightkdata.mongowp.server.api.oplog.OplogOperation;

import java.util.Objects;


public interface OplogOperationFilter extends Filter<OplogOperation> {

  @Override
  public FilterResult<OplogOperation> apply(OplogOperation op);

  @Override
  public default OplogOperationFilter and(Filter<OplogOperation> other) {
    Objects.requireNonNull(other);
    return (e) -> apply(e).and(other.apply(e));
  }

}
