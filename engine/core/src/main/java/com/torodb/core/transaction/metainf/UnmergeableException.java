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

package com.torodb.core.transaction.metainf;

import com.torodb.core.transaction.RollbackException;

/**
 *
 */
public class UnmergeableException extends RollbackException {

  private static final long serialVersionUID = 6944819560068647762L;

  private final transient ImmutableMetaSnapshot currentSnapshot;
  private final transient MutableMetaSnapshot newSnapshot;

  public UnmergeableException(ImmutableMetaSnapshot currentSnapshot,
      MutableMetaSnapshot newSnapshot) {
    this.currentSnapshot = currentSnapshot;
    this.newSnapshot = newSnapshot;
  }

  public UnmergeableException(ImmutableMetaSnapshot currentSnapshot,
      MutableMetaSnapshot newSnapshot, String message) {
    super(message);
    this.currentSnapshot = currentSnapshot;
    this.newSnapshot = newSnapshot;
  }

  public UnmergeableException(ImmutableMetaSnapshot currentSnapshot,
      MutableMetaSnapshot newSnapshot, String message, Throwable cause) {
    super(message, cause);
    this.currentSnapshot = currentSnapshot;
    this.newSnapshot = newSnapshot;
  }

  public ImmutableMetaSnapshot getCurrentSnapshot() {
    return currentSnapshot;
  }

  public MutableMetaSnapshot getNewSnapshot() {
    return newSnapshot;
  }

}
