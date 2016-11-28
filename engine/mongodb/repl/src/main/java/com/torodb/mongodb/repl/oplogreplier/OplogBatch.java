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

package com.torodb.mongodb.repl.oplogreplier;

import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.torodb.mongodb.repl.RecoveryService;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

/**
 *
 */
public interface OplogBatch {

  /**
   * Returns the list of operations contained by this batch.
   *
   * If {@link #isLastOne() } returns true, then this method will return an empty list.
   *
   * @return
   */
  public List<OplogOperation> getOps();

  /**
   * Returns true if the {@link OplogFetcher} that created this batch thinks that there are more
   * elements that can be fetch right now from the remote oplog.
   *
   * @return
   */
  public boolean isReadyForMore();

  /**
   * Returns true if this batch has been fetch after the producer {@link OplogFetcher} thinks it has
   * finished the remote oplog.
   *
   * This could happen when the fetcher was created with a given limit, which is usual on the
   * context of a {@link RecoveryService}, or when the fetcher is closed for any reason.
   *
   * If a batch returns true to this method, then it cannot contain any that, so {@link #getOps() }
   * will return an empty stream.
   *
   * @return
   */
  public boolean isLastOne();

  @Nullable
  public default OplogOperation getLastOperation() {
    if (getOps().isEmpty()) {
      return null;
    }
    return getOps().get(getOps().size() - 1);
  }

  public default int count() {
    return getOps().size();
  }

  public default boolean isEmpty() {
    return getOps().isEmpty() || isLastOne();
  }

  public default OplogBatch concat(OplogBatch next) {
    ArrayList<OplogOperation> newList = new ArrayList<>(this.count() + next.count());
    newList.addAll(getOps());
    newList.addAll(next.getOps());
    return new NormalOplogBatch(newList, next.isReadyForMore());
  }

}
